;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC Sucursal en España SL

(ns backend-tests.rpc-file-gc-repair-test
  "Regression tests for file-gc on files whose stored data fails schema
  validation (see https://github.com/penpot/penpot/issues/12433).

  Both tests are written in their final form: they assert that file-gc
  repairs the file and completes. They are RED until the repair work is
  implemented (run-task! returns nil and the handler logs
  `hint=\"error on cleaning file\"` with `invalid file data`)."
  (:require
   [app.binfile.common :as bfc]
   [app.common.features :as cfeat]
   [app.common.files.validate :as cfv]
   [app.common.types.shape :as cts]
   [app.common.uuid :as uuid]
   [app.db :as db]
   [app.rpc :as-alias rpc]
   [backend-tests.helpers :as th]
   [clojure.test :as t]))

(t/use-fixtures :once th/state-init)
(t/use-fixtures :each th/database-reset)

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

    ;; Pin the failure cause: the file no longer even reads.
    (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"invalid file data"
            (read-file file-id)))

    (t/testing "file-gc repairs nil pages and completes"
      (t/is (true? (th/run-task! :file-gc {:file-id file-id})))
      (t/is (true? (:has-media-trimmed (th/db-get :file {:id file-id})))))))
