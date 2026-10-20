;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns backend-tests.rpc-file-gc-repair-test
  "Regression tests for file-gc on files whose stored data fails schema
  validation (see https://github.com/penpot/penpot/issues/12433)."
  (:require
   [app.binfile.common :as bfc]
   [app.common.features :as cfeat]
   [app.common.files.validate :as cfv]
   [app.common.logging :as l]
   [app.common.time :as ct]
   [app.common.types.shape :as cts]
   [app.common.uuid :as uuid]
   [app.db :as db]
   [app.rpc :as-alias rpc]
   [app.tasks.file-gc :as file-gc]
   [backend-tests.helpers :as th]
   [clojure.test :as t]))

(t/use-fixtures :once th/state-init)
(t/use-fixtures :each th/database-reset)

(defn- wait-for-log
  "First captured log record matching pred, waiting up to the
  deadline because logging is asynchronous."
  [records pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (if-let [record (first (filter pred @records))]
        record
        (when (< (System/currentTimeMillis) deadline)
          (Thread/sleep 10)
          (recur))))))

(defn- error-with-hint
  "Log record predicate: an error log with the given hint."
  [hint]
  (fn [record]
    (let [props (:app.common.logging/props record)]
      (and (= :error (:app.common.logging/level record))
           (= hint (get props :hint))))))

(defn- apply-file-changes
  [& {:keys [profile-id file-id changes revn] :or {revn 0}}]
  (let [params {::th/type :update-file
                ::rpc/profile-id profile-id
                :id file-id
                :session-id (uuid/random)
                :revn revn
                :vern 0
                :features cfeat/supported-features
                :changes changes}
        out    (th/command! params)]
    (t/is (nil? (:error out)))
    (:result out)))

(defn- read-file
  [file-id]
  (db/run! th/*system*
           (fn [cfg]
             (bfc/get-file cfg file-id :realize? true))))

(defn- persist-file
  [file]
  (db/tx-run! th/*system*
              (fn [cfg]
                (bfc/update-file! cfg file))))

(defn- corrupt-shape-types
  "Mirrors production files where the decoder left unconverted types:
  shape `:type` as text, ids as text and object/page-map keys as
  text/keyword instead of uuid."
  [data]
  (letfn [(corrupt-shape [shape]
            (cond-> shape
              (keyword? (:type shape))
              (assoc :type (name (:type shape)))

              (uuid? (:id shape))
              (assoc :id (str (:id shape)))

              (uuid? (:parent-id shape))
              (update :parent-id str)

              (uuid? (:frame-id shape))
              (update :frame-id str)))
          (corrupt-objects [objects]
            (into {}
                  (map (fn [[id shape]]
                         [(keyword (str id)) (corrupt-shape shape)]))
                  objects))
          (corrupt-page [[page-id page]]
            [(str page-id) (update page :objects corrupt-objects)])]
    (-> data
        (update :pages (fn [pages] (mapv str pages)))
        (update :pages-index (fn [pages-index]
                               (into {} (map corrupt-page) pages-index))))))

(defn- corrupt-with-nil-page
  "Mirrors production files where the `:pages` vector holds nil and
  `:pages-index` holds a nil key (see legacy-57, already applied on
  those files, so the nils came back through a write path)."
  [data]
  (-> data
      (update :pages conj nil)
      (assoc-in [:pages-index nil] {:id nil :name "ghost" :objects {}})))

(defn- setup-file-with-rect
  [profile file]
  (let [page-id  (first (:pages (:data file)))
        shape-id (uuid/next)]
    (apply-file-changes
     :profile-id (:id profile)
     :file-id (:id file)
     :changes [{:type :add-obj
                :page-id page-id
                :id shape-id
                :parent-id uuid/zero
                :frame-id uuid/zero
                :components-v2 true
                :obj (cts/setup-shape
                      {:id shape-id
                       :name "rect"
                       :frame-id uuid/zero
                       :parent-id uuid/zero
                       :type :rect})}])
    (:id file)))

(defn- instantiate-component
  "Adds a copy of the library component to the given file."
  [profile file library-id component-id]
  (let [page-id  (first (:pages (:data file)))
        shape-id (uuid/next)]
    (apply-file-changes
     :profile-id (:id profile)
     :file-id (:id file)
     :changes [{:type :add-obj
                :page-id page-id
                :id shape-id
                :parent-id uuid/zero
                :frame-id uuid/zero
                :components-v2 true
                :obj (cts/setup-shape
                      {:id shape-id
                       :name "Board"
                       :frame-id uuid/zero
                       :parent-id uuid/zero
                       :type :frame
                       :main-instance false
                       :component-root true
                       :component-file library-id
                       :component-id component-id})}])
    shape-id))

(defn- add-library-component
  "Adds a component (with its main instance) to the library file.
  Returns the map with the main shape id and the component id."
  [profile library component-name]
  (let [lib-page-id (first (get-in library [:data :pages]))
        shape-id    (uuid/next)
        component-id (uuid/next)]
    (apply-file-changes
     :profile-id (:id profile)
     :file-id (:id library)
     :changes [{:type :add-obj
                :page-id lib-page-id
                :id shape-id
                :parent-id uuid/zero
                :frame-id uuid/zero
                :components-v2 true
                :obj (cts/setup-shape
                      {:id shape-id
                       :name "Board"
                       :frame-id uuid/zero
                       :parent-id uuid/zero
                       :type :frame
                       :main-instance true
                       :component-root true
                       :component-file (:id library)
                       :component-id component-id})}
               {:type :add-component
                :path ""
                :name component-name
                :main-instance-id shape-id
                :main-instance-page lib-page-id
                :id component-id
                :anotation nil}])
    {:shape-id shape-id :component-id component-id :page-id lib-page-id}))

(t/deftest file-gc-cleans-library-used-by-several-files
  (let [profile (th/create-profile* 1)
        library (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared true})
        users   (mapv (fn [i]
                        (th/create-file* i {:profile-id (:id profile)
                                            :project-id (:default-project-id profile)
                                            :is-shared false}))
                      [2 3 4])
        _       (doseq [user users]
                  (th/link-file-to-library*
                   {:file-id (:id user)
                    :library-id (:id library)}))
        used    (add-library-component profile library "Used")
        unused  (add-library-component profile library "Unused")]

    ;; Only the used component is instantiated in the user files.
    (doseq [user users]
      (instantiate-component profile user (:id library) (:component-id used)))

    ;; Delete both components (and their mains) from the library.
    (doseq [{:keys [component-id page-id shape-id]} [used unused]]
      (apply-file-changes
       :profile-id (:id profile)
       :file-id (:id library)
       :changes [{:type :del-component :id component-id}
                 {:type :del-obj :page-id page-id :id shape-id
                  :ignore-touched true}]))

    ;; The scan over library users must not hold a write transaction
    ;; open (it used to hold a db/plan cursor while loading every
    ;; user file, tripping idle-in-transaction timeouts in prod).
    (t/testing "file-gc cleans a library used by several files"
      (t/is (true? (th/run-task! :file-gc {:file-id (:id library)})))
      (let [components (get-in (read-file (:id library)) [:data :components])]
        (t/is (contains? components (:component-id used))
              "component still used by library users is preserved")
        (t/is (not (contains? components (:component-id unused)))
              "component used nowhere is deleted"))
      (t/is (true? (:has-media-trimmed (th/db-get :file {:id (:id library)})))))))

(t/deftest file-gc-repairs-unconverted-schema-types-before-cleaning
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)]

    ;; Corrupt the stored data the way production files look today and
    ;; persist it through the real (non-validating) write path.
    (-> (read-file file-id)
        (update :data corrupt-shape-types)
        (persist-file))

    ;; Pin the failure cause: the persisted file is schema-invalid.
    (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid file data"
                            (cfv/validate-file-schema! (read-file file-id))))

    ;; The file reads fine (no migration pending) but must fail at
    ;; clean-media validation until the repair is implemented.
    (t/testing "file-gc repairs schema-invalid types and completes"
      (t/is (true? (th/run-task! :file-gc {:file-id file-id})))
      (t/is (true? (:has-media-trimmed (th/db-get :file {:id file-id})))))))

(t/deftest file-gc-repairs-nil-pages-that-fail-at-migration
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)]

    (-> (read-file file-id)
        (update :data corrupt-with-nil-page)
        (persist-file))

    ;; Force the read-time migration check by dropping the applied
    ;; migrations, mirroring production files that fail inside
    ;; get-file/migrate-file before cleaning ever starts.
    (th/db-delete! :file-migration {:file-id file-id})

    ;; Since the 0027 migration heals nil pages, the read now
    ;; succeeds where it used to throw invalid file data.

    (t/testing "file-gc repairs nil pages and completes"
      (t/is (true? (th/run-task! :file-gc {:file-id file-id})))
      (t/is (true? (:has-media-trimmed (th/db-get :file {:id file-id})))))))

(defn- corrupt-with-unknown-type
  "Corruption no migration can heal: an unknown shape type. The file
  must keep failing loudly instead of being silently rewritten."
  [data page-id shape-id]
  (assoc-in data [:pages-index page-id :objects shape-id :type] "triangle"))

(t/deftest file-gc-returns-false-for-unfixable-file-without-half-tx
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)
        file    (read-file file-id)
        page-id (first (:pages (:data file)))
        shape-id (first (remove #{uuid/zero}
                                (filter uuid? (keys (get-in file [:pages-index page-id :objects])))))]

    (-> (read-file file-id)
        (update :data #(corrupt-with-unknown-type % page-id shape-id))
        (persist-file))

    ;; Force the read-time migration check: neither the normal path
    ;; nor the explicit-migration fallback can fix an unknown type.
    (th/db-delete! :file-migration {:file-id file-id})

    (let [revn-before (:revn (th/db-get :file {:id file-id}))
          records (atom [])
          _ (add-watch l/log-record ::capture
                       (fn [_ _ _ record] (swap! records conj record)))]
      (try
        (let [result (th/run-task! :file-gc {:file-id file-id})
              ;; Keep the watch installed while waiting: log emission
              ;; is asynchronous, removing it right after run-task!
              ;; would drop the record before it arrives.
              repairing (wait-for-log records (error-with-hint "error on repairing file"))]
          ;; The handler catches the migration failure and returns nil,
          ;; like any other cleaning error: loud in Loki, retried by the
          ;; scheduler, never half-persisted.
          (t/testing "file-gc gives up cleanly on unfixable files"
            (t/is (nil? result))
            (t/is (false? (:has-media-trimmed (th/db-get :file {:id file-id})))
                  "file is not marked as trimmed")
            (t/is (= revn-before (:revn (th/db-get :file {:id file-id})))
                  "no half transaction was persisted")
            (t/is (some? repairing)
                  "the failure is reported as a Tx1 error")
            ;; The retry failure chains the original read cause so Loki
            ;; shows both with the default level, not just the second.
            (let [cause (::l/cause repairing)
                  data (ex-data cause)]
              (t/is (= :tx1-read (:phase data))
                    "the retry wrapper tags the Tx1 read phase")
              (t/is (= (str file-id) (:file-id data)))
              (t/is (some? (:retry-cause data))
                    "the retry failure is kept in ex-data")
              (t/is (some? (.getCause ^Throwable cause))
                    "the original read cause is chained"))
            ;; Tx2 never runs, so no cleaning log is ever queued: check
            ;; the captured records directly instead of waiting out the
            ;; full deadline for something that will never arrive.
            (t/is (nil? (some (error-with-hint "error on cleaning file") @records))
                  "Tx2 never runs, so it reports nothing")))
        (finally
          (remove-watch l/log-record ::capture))))))

(defn- corrupt-with-broken-child
  "Semantic breakage that repair-file heals: a dangling child id on the
  root shape. Validation reports :child-not-found and the repair drops
  the id, so the file is clean afterwards."
  [data page-id]
  (update-in data [:pages-index page-id :objects uuid/zero :shapes] conj (uuid/next)))

(t/deftest file-gc-tx2-aborts-when-file-changed
  ;; White-box: the handler always runs Tx1 and Tx2 back to back, so the
  ;; revn-abort is only isolable by calling clean-file-in-tx directly.
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)
        page-id (first (:pages (:data (read-file file-id))))]

    (-> (read-file file-id)
        (update :data #(corrupt-with-broken-child % page-id))
        (persist-file))

    (let [repaired (#'file-gc/repair-and-persist th/*system* {:file-id file-id})]
      (t/is (some? repaired) "Tx1 repairs and returns the file")
      (let [revn-after-tx1 (:revn repaired)]
        ;; Concurrent write after Tx1: bump revn through the real RPC path.
        (apply-file-changes
         :profile-id (:id profile)
         :file-id file-id
         :revn revn-after-tx1
         :changes [{:type :mod-page :id page-id :name "bumped"}])
        (let [revn-after-bump (:revn (th/db-get :file {:id file-id}))
              result          (#'file-gc/clean-file-in-tx th/*system* repaired)]
          (t/testing "Tx2 aborts on stale revn without writing"
            (t/is (false? result))
            (t/is (false? (:has-media-trimmed (th/db-get :file {:id file-id})))
                  "the file is not marked as trimmed")
            (t/is (= revn-after-bump (:revn (th/db-get :file {:id file-id})))
                  "Tx2 wrote nothing")
            (t/is (> revn-after-bump revn-after-tx1)
                  "the concurrent bump is still there")))))))

(t/deftest file-gc-tx2-skips-when-file-deleted
  ;; White-box like the stale-revn test: delete the file after Tx1 and
  ;; call clean-file-in-tx directly with the Tx1 snapshot. The light row lock
  ;; sees the deleted row and Tx2 skips without writing.
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)]
    (let [repaired (#'file-gc/repair-and-persist th/*system* {:file-id file-id})]
      (t/is (some? repaired) "Tx1 reads and returns the file")
      (th/db-update! :file {:deleted-at (ct/now)} {:id file-id})
      (let [result (#'file-gc/clean-file-in-tx th/*system* repaired)
            row    (th/db-get :file {:id file-id} {::db/remove-deleted false})]
        (t/testing "Tx2 skips a file deleted after Tx1 without writing"
          (t/is (false? result))
          (t/is (some? (:deleted-at row)) "still deleted")
          (t/is (false? (:has-media-trimmed row))
                "the file is not marked as trimmed"))))))

(t/deftest file-gc-tx1-repair-survives-tx2-failure
  ;; White-box: Tx1 commits on its own; a later Tx2 failure must not roll
  ;; it back. Only direct repair-and-persist calls can observe the commit
  ;; in between, the handler always runs both phases together.
  (let [profile (th/create-profile* 1)
        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id (:default-project-id profile)
                                    :is-shared false})
        file-id (setup-file-with-rect profile file)
        file    (read-file file-id)
        page-id (first (:pages (:data file)))
        shape-id (first (remove #{uuid/zero}
                                (filter uuid? (keys (get-in file [:pages-index page-id :objects])))))]

    (-> (read-file file-id)
        (update :data #(corrupt-with-broken-child % page-id))
        (persist-file))

    (let [repaired (#'file-gc/repair-and-persist th/*system* {:file-id file-id})]
      (t/is (some? repaired) "Tx1 repairs and returns the file")
      (let [revn-after-tx1 (:revn (th/db-get :file {:id file-id}))]
        (t/is (> revn-after-tx1 (:revn file))
              "Tx1 persisted the repair with a revn bump")
        (t/is (nil? (cfv/validate-file (read-file file-id) {}))
              "the persisted file validates clean")

        ;; Superpose corruption no migration can heal, through the direct
        ;; (non-validating) write path, like production saves do. Drop
        ;; the migration row so the next read runs the migration check
        ;; and fails, like the unfixable-file test does.
        (-> (read-file file-id)
            (update :data #(corrupt-with-unknown-type % page-id shape-id))
            (persist-file))
        (th/db-delete! :file-migration {:file-id file-id})
        (let [revn-corrupted (:revn (th/db-get :file {:id file-id}))]

          ;; The full run fails in Tx1 and never marks the file, but the
          ;; Tx1 commit from before is still the base: revn never goes
          ;; backwards and nothing is rolled back across transactions.
          (t/testing "a later Tx2 failure keeps the Tx1 commit"
            (t/is (nil? (th/run-task! :file-gc {:file-id file-id})))
            (t/is (false? (:has-media-trimmed (th/db-get :file {:id file-id})))
                  "the file is not marked as trimmed")
            (t/is (= revn-corrupted (:revn (th/db-get :file {:id file-id})))
                  "the failed run wrote nothing")
            (t/is (>= revn-corrupted revn-after-tx1)
                  "revn never went backwards: no cross-transaction rollback")))))))
