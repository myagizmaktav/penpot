;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns backend-tests.rpc-file-update-test
  (:require
   [app.common.exceptions :as ex]
   [app.common.features :as cfeat]
   [app.common.files.changes :as cpc]
   [app.common.files.validate :as val]
   [app.common.types.shape :as cts]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.msgbus :as mbus]
   [app.rpc :as-alias rpc]
   [app.util.blob :as blob]
   [backend-tests.helpers :as th]
   [clojure.test :as t]))

(defn- with-validation
  [next]
  (with-redefs [cf/flags (conj cf/flags :file-validation :file-schema-validation)]
    (next)))

(t/use-fixtures :once th/state-init)
(t/use-fixtures :each th/database-reset with-validation)

(defn- create-file
  []
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})]
    {:profile-id (:id profile)
     :file-id    (:id file)
     :page-id    (-> file :data :pages first)}))

(defn- add-rect
  [page-id id]
  {:type :add-obj
   :page-id page-id
   :id id
   :parent-id uuid/zero
   :frame-id uuid/zero
   :obj (cts/setup-shape {:id id
                          :type :rect
                          :name "rect"
                          :x 0 :y 0 :width 10 :height 10
                          :parent-id uuid/zero
                          :frame-id uuid/zero})})

(defn- break-parent
  "A change that leaves the shape pointing to a parent that does not exist."
  [page-id id]
  {:type :mod-obj
   :page-id page-id
   :id id
   :operations [{:type :set :attr :parent-id :val (uuid/next)}]})

(defn- update-file
  [{:keys [profile-id file-id]} params]
  (th/command! (merge {::th/type :update-file
                       ::rpc/profile-id profile-id
                       :id file-id
                       :session-id (uuid/next)
                       :revn 0
                       :vern 0
                       :features cfeat/supported-features}
                      params)))

(defn- commit
  [& changes]
  {:commit-id (uuid/next) :changes (vec changes)})

(defn- get-file-data
  [{:keys [profile-id file-id]}]
  (-> (th/command! {::th/type :get-file
                    ::rpc/profile-id profile-id
                    :id file-id})
      :result
      :data))

(defn- get-file-changes
  [{:keys [file-id]}]
  (->> (th/db-query :file-change {:file-id file-id} {:order-by [:revn]})
       (filter :changes)
       (mapv #(update % :changes blob/decode))))

(defn- capture-messages
  [next]
  (let [messages (atom [])]
    (with-redefs [mbus/pub! (fn [_ & {:keys [message]}]
                              (swap! messages conj message))]
      (let [out (next)]
        {:out out :messages @messages}))))

(t/deftest saves-the-commits-before-the-first-invalid-one
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        shape-c (uuid/next)
        commit1 (commit (add-rect page-id shape-a))
        commit2 (commit (break-parent page-id shape-a))
        commit3 (commit (add-rect page-id shape-c))

        {:keys [out messages]}
        (capture-messages
         #(update-file ctx {:changes-with-metadata [commit1 commit2 commit3]}))]

    (t/is (nil? (:error out)))

    (t/testing "the response names the rejected commits and the failure"
      (let [rejected (-> out :result :rejected)]
        (t/is (= [(:commit-id commit2) (:commit-id commit3)] (:commit-ids rejected)))
        (t/is (= (:commit-id commit2) (:failed-commit-id rejected)))
        (t/is (= :validation (-> rejected :error :type)))
        (t/is (= :referential-integrity (-> rejected :error :code)))))

    (t/testing "the file holds the accepted commit only"
      (let [objects (-> (get-file-data ctx) :pages-index (get page-id) :objects)]
        (t/is (= uuid/zero (get-in objects [shape-a :parent-id])))
        (t/is (not (contains? objects shape-c)))))

    (t/testing "the change log holds the accepted commit only"
      (let [rows (get-file-changes ctx)]
        (t/is (= 1 (count rows)))
        (t/is (= 1 (:revn (first rows))))
        (t/is (= (:changes commit1) (:changes (first rows))))))

    (t/testing "peers get the accepted commit only"
      (let [file-changes (filter #(= :file-change (:type %)) messages)]
        (t/is (= 1 (count file-changes)))
        (t/is (= (:changes commit1) (:changes (first file-changes))))))))

(t/deftest saves-nothing-when-the-first-commit-is-invalid
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        _       (update-file ctx {:changes [(add-rect page-id shape-a)]})
        commit1 (commit (break-parent page-id shape-a))
        commit2 (commit (add-rect page-id (uuid/next)))

        {:keys [out messages]}
        (capture-messages
         #(update-file ctx {:revn 1 :changes-with-metadata [commit1 commit2]}))]

    (t/is (nil? (:error out)))
    (t/is (= [(:commit-id commit1) (:commit-id commit2)]
             (-> out :result :rejected :commit-ids)))
    (t/is (= 1 (:revn (th/db-get :file {:id (:file-id ctx)}))))
    (t/is (= 1 (count (get-file-changes ctx))))
    (t/is (empty? messages))))

(t/deftest rejects-a-commit-that-throws-while-applied
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        commit1 (commit (add-rect page-id shape-a))
        ;; `:add-page` refuses an id+name and a page at once
        commit2 (commit {:type :add-page
                         :id (uuid/next)
                         :name "page"
                         :page {:id (uuid/next) :name "page" :objects {}}})
        out     (update-file ctx {:changes-with-metadata [commit1 commit2]})]

    (t/is (nil? (:error out)))
    (t/is (= [(:commit-id commit2)] (-> out :result :rejected :commit-ids)))
    (t/is (= :conflict (-> out :result :rejected :error :type)))
    (t/is (= 1 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(t/deftest rejects-a-commit-that-only-fails-the-schema
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a  (uuid/next)
        shape-c  (uuid/next)
        commit1  (commit (add-rect page-id shape-a))
        commit2  (commit {:type :mod-obj
                          :page-id page-id
                          :id shape-a
                          :operations [{:type :set :attr :name :val "schema-breaker"}]})
        commit3  (commit (add-rect page-id shape-c))
        validate val/validate-file-schema!
        ;; Changes and shapes are schema checked while applied, so no real
        ;; change reaches the file schema check invalid: one is simulated
        out      (with-redefs [val/validate-file-schema!
                               (fn [file]
                                 (when (= "schema-breaker"
                                          (get-in file [:data :pages-index page-id :objects shape-a :name]))
                                   (ex/raise :type :validation :code :schema-validation :hint "schema"))
                                 (validate file))]
                   (update-file ctx {:changes-with-metadata [commit1 commit2 commit3]}))
        objects  (-> (get-file-data ctx) :pages-index (get page-id) :objects)]

    (t/is (nil? (:error out)))
    (t/is (= [(:commit-id commit2) (:commit-id commit3)]
             (-> out :result :rejected :commit-ids)))
    (t/is (= :schema-validation (-> out :result :rejected :error :code)))
    (t/is (= "rect" (get-in objects [shape-a :name])))
    (t/is (not (contains? objects shape-c)))))

(t/deftest saves-every-commit-when-all-are-valid
  (let [{:keys [page-id] :as ctx} (create-file)
        out (update-file ctx {:changes-with-metadata [(commit (add-rect page-id (uuid/next)))
                                                      (commit (add-rect page-id (uuid/next)))]})]
    (t/is (nil? (:error out)))
    (t/is (nil? (-> out :result :rejected)))
    (t/is (= 1 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(t/deftest flat-changes-stay-all-or-nothing
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        out     (update-file ctx {:changes [(add-rect page-id shape-a)
                                            (break-parent page-id shape-a)]})]
    (t/is (= :validation (th/ex-type (:error out))))
    (t/is (= 0 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(t/deftest groups-without-commit-ids-stay-all-or-nothing
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        out     (update-file ctx {:changes-with-metadata
                                  [{:changes [(add-rect page-id shape-a)]}
                                   {:changes [(break-parent page-id shape-a)]}]})]
    (t/is (= :validation (th/ex-type (:error out))))
    (t/is (= 0 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(t/deftest database-failures-fail-the-whole-request
  (let [{:keys [page-id] :as ctx} (create-file)
        process cpc/process-changes
        calls   (atom 0)
        out     (with-redefs [cpc/process-changes
                              (fn [& args]
                                ;; The first call is the single pass over
                                ;; every commit; the database fails on the
                                ;; first attempt of the retry. The 3-arity
                                ;; call is the same pass.
                                (when (and (= 2 (count args))
                                           (= 2 (swap! calls inc)))
                                  (throw (java.sql.SQLException. "connection lost")))
                                (apply process args))]
                  (update-file ctx {:changes-with-metadata
                                    [(commit (add-rect page-id (uuid/next)))
                                     (commit {:type :add-page
                                              :id (uuid/next)
                                              :name "page"
                                              :page {:id (uuid/next) :name "page" :objects {}}})]}))]
    (t/is (instance? java.sql.SQLException (:error out)))
    (t/is (= 0 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(defn- move-rect
  [page-id id x]
  {:type :mod-obj
   :page-id page-id
   :id id
   :operations [{:type :set :attr :x :val x}]})

(defn- update-file-counting
  "Updates the file and counts the times the changes are applied."
  [ctx params]
  (let [process cpc/process-changes
        calls   (atom 0)
        out     (with-redefs [cpc/process-changes
                              (fn [& args]
                                ;; The 3-arity call is the same application
                                (when (= 2 (count args))
                                  (swap! calls inc))
                                (apply process args))]
                  (update-file ctx params))]
    {:out out :calls @calls}))

(t/deftest bisects-the-commits-to-find-the-invalid-one
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        _       (update-file ctx {:changes [(add-rect page-id shape-a)]})
        commits (mapv (fn [i]
                        (if (= i 5)
                          (commit (break-parent page-id shape-a))
                          (commit (move-rect page-id shape-a i))))
                      (range 16))

        {:keys [out calls]}
        (update-file-counting ctx {:revn 1 :changes-with-metadata commits})]

    (t/is (nil? (:error out)))
    (t/is (= (mapv :commit-id (subvec commits 5))
             (-> out :result :rejected :commit-ids)))
    (t/is (= (:commit-id (nth commits 5))
             (-> out :result :rejected :failed-commit-id)))
    (t/is (= :referential-integrity (-> out :result :rejected :error :code)))

    (t/testing "the single pass plus log2 of the commits attempts"
      (t/is (= 5 calls)))

    (t/testing "the file holds the commits before the invalid one"
      (let [objects (-> (get-file-data ctx) :pages-index (get page-id) :objects)]
        (t/is (= uuid/zero (get-in objects [shape-a :parent-id])))
        (t/is (= 4 (get-in objects [shape-a :x])))))))

(t/deftest rejects-a-single-commit-without-a-retry
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        _       (update-file ctx {:changes [(add-rect page-id shape-a)]})
        commit1 (commit (break-parent page-id shape-a))

        {:keys [out calls]}
        (update-file-counting ctx {:revn 1 :changes-with-metadata [commit1]})]

    (t/is (nil? (:error out)))
    (t/is (= [(:commit-id commit1)] (-> out :result :rejected :commit-ids)))
    (t/is (= :referential-integrity (-> out :result :rejected :error :code)))
    (t/is (= 1 calls))
    (t/is (= 1 (:revn (th/db-get :file {:id (:file-id ctx)}))))))

(t/deftest keeps-a-commit-that-repairs-an-earlier-one
  (let [{:keys [page-id] :as ctx} (create-file)
        shape-a (uuid/next)
        shape-c (uuid/next)
        _       (update-file ctx {:changes [(add-rect page-id shape-a)]})
        commit1 (commit (break-parent page-id shape-a))
        commit2 (commit {:type :mod-obj
                         :page-id page-id
                         :id shape-a
                         :operations [{:type :set :attr :parent-id :val uuid/zero}]})
        commit3 (commit (add-rect page-id shape-c))
        commit4 (commit (break-parent page-id shape-c))
        out     (update-file ctx {:revn 1 :changes-with-metadata [commit1 commit2 commit3 commit4]})
        objects (-> (get-file-data ctx) :pages-index (get page-id) :objects)]

    (t/is (nil? (:error out)))
    (t/is (= [(:commit-id commit4)] (-> out :result :rejected :commit-ids)))
    (t/is (= uuid/zero (get-in objects [shape-a :parent-id])))
    (t/is (= uuid/zero (get-in objects [shape-c :parent-id])))))
