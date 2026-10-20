;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns backend-tests.tasks-team-storage-test
  "Cached per-team media storage usage fed by the audit-log
  storage-usage consumer: selective queueing, dirty marking, bounded
  recompute and quota reads."
  (:require
   [app.common.time :as ct]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.db :as db]
   [app.loggers.audit :as audit]
   [app.loggers.audit.consumer-task :as consumer]
   [app.rpc :as-alias rpc]
   [app.rpc.quotes :as quotes]
   [backend-tests.helpers :as th]
   [clojure.test :as t]
   [mockery.core :refer [with-mocks]]))

(t/use-fixtures :once th/state-init)
(t/use-fixtures :each th/database-reset)

(def ^:private test-flags
  "Quote checks run (:quotes), events are stored (:audit-log) and the
  storage-usage consumer queues (:storage-usage)."
  #{:quotes :audit-log :storage-usage})

(defn- storage-usage-pendings
  []
  (th/db-exec! ["SELECT audit_log_id, consumer FROM audit_log_pending WHERE consumer = 3"]))

(defn- usage-row
  [team-id]
  (th/db-exec-one! ["SELECT team_id, total_bytes, dirty_at FROM team_storage_usage WHERE team_id = ?" team-id]))

(defn- insert-row!
  [name source]
  (th/db-insert! :audit-log
                 {:id         (uuid/next)
                  :name       name
                  :type       "action"
                  :source     source
                  :profile-id uuid/zero
                  :ip-addr    (db/inet "127.0.0.1")
                  :props      (db/tjson {})
                  :context    (db/tjson {})
                  :tracked-at (ct/now)
                  :created-at (ct/now)}))

(defn- append-row!
  [name props]
  (#'audit/append-audit-entry
   th/*system*
   {:id         (uuid/next)
    :name       name
    :type       "action"
    :source     "backend"
    :profile-id uuid/zero
    :ip-addr    "127.0.0.1"
    :props      props
    :context    {}
    :tracked-at (ct/now)
    :created-at (ct/now)}))

(t/deftest storage-usage-queueing-matches-listed-names
  (with-redefs [cf/flags #{:audit-log :storage-usage}]
    (let [r1 (insert-row! "upload-file-media-object" "backend")
          r2 (insert-row! "navigate" "backend")
          r3 (insert-row! "delete-file" "backend")]
      (consumer/queue-storage-usage-pendings
       th/*system*
       [(:id r1) (:id r2) (:id r3)]
       ["upload-file-media-object" "navigate" "delete-file"]
       ["backend" "backend" "backend"])
      (t/is (= 2 (count (storage-usage-pendings)))))))

(t/deftest storage-usage-queueing-skips-telemetry-shadows
  (with-redefs [cf/flags #{:audit-log :telemetry :storage-usage}]
    (let [r1 (insert-row! "upload-file-media-object" "backend")
          r2 (insert-row! "upload-file-media-object" "telemetry:backend")]
      (consumer/queue-storage-usage-pendings
       th/*system*
       [(:id r1) (:id r2)]
       ["upload-file-media-object" "upload-file-media-object"]
       ["backend" "telemetry:backend"])
      (t/is (= 1 (count (storage-usage-pendings)))))))

(t/deftest storage-usage-queueing-needs-flag
  (with-redefs [cf/flags #{:audit-log}]
    (let [r1 (insert-row! "upload-file-media-object" "backend")]
      (consumer/queue-storage-usage-pendings
       th/*system*
       [(:id r1)]
       ["upload-file-media-object"]
       ["backend"])
      (t/is (= 0 (count (storage-usage-pendings)))))))

(t/deftest storage-usage-append-queues-selectively
  (with-redefs [cf/flags test-flags]
    (append-row! "upload-file-media-object" {:team-id (uuid/next)})
    (append-row! "navigate" {:route "dashboard"})
    (t/is (= 1 (count (storage-usage-pendings))))))

(t/deftest storage-usage-drain-and-recompute
  (let [prof (th/create-profile* 1)
        team-id (:default-team-id prof)
        proj (th/create-project* 1 {:profile-id (:id prof) :team-id team-id})
        file (th/create-file* 1 {:profile-id (:id prof) :project-id (:id proj) :is-shared false})
        mfile {:filename "sample.jpg"
               :path (th/tempfile "backend_tests/test_files/sample.jpg")
               :mtype "image/jpeg"
               :size 312043}]
    (with-redefs [cf/flags test-flags]
      (let [out (th/multipart-command! {::th/type :upload-file-media-object
                                        ::rpc/profile-id (:id prof)
                                        :file-id (:id file)
                                        :is-local true
                                        :name "testfile"
                                        :content mfile})]
        (t/is (nil? (:error out))))
      (append-row! "upload-file-media-object" {:team-id team-id :file-id (:id file)})
      (th/run-task! :audit-log-consumer-storage-usage {})
      (let [row (usage-row team-id)]
        (t/is (some? row))
        (t/is (some? (:dirty-at row))))
      (th/run-task! :recompute-team-storage {:chunk-size 10})
      (let [row (usage-row team-id)
            live (quotes/get-live-media-storage-bytes th/*system* team-id)]
        (t/is (pos? (:total-bytes row)))
        (t/is (== live (:total-bytes row)))
        (t/is (nil? (:dirty-at row))))
      (with-mocks [mock {:target 'app.config/get
                         :return (th/config-get-mock {:quotes-media-storage-bytes-per-team 100})}]
        (let [out (th/multipart-command! {::th/type :upload-file-media-object
                                          ::rpc/profile-id (:id prof)
                                          :file-id (:id file)
                                          :is-local true
                                          :name "testfile2"
                                          :content mfile})]
          (t/is (some? (:error out)))
          (t/is (= :max-quote-reached (:code (ex-data (:error out))))))))))

(t/deftest storage-usage-missing-row-fails-open
  (let [prof (th/create-profile* 1)
        team-id (:default-team-id prof)]
    (with-redefs [cf/flags test-flags]
      (quotes/check! th/*system* {::quotes/id ::quotes/media-storage-bytes-per-team
                                  ::quotes/profile-id (:id prof)
                                  ::quotes/team-id team-id
                                  ::quotes/incr 0})
      (let [row (usage-row team-id)]
        (t/is (some? row))
        (t/is (some? (:dirty-at row)))
        (t/is (= 0 (:total-bytes row)))))))

(t/deftest storage-usage-file-gc-emits-event
  (let [prof (th/create-profile* 1)
        team-id (:default-team-id prof)
        proj (th/create-project* 1 {:profile-id (:id prof) :team-id team-id})
        file (th/create-file* 1 {:profile-id (:id prof) :project-id (:id proj) :is-shared false})
        mfile {:filename "sample.jpg"
               :path (th/tempfile "backend_tests/test_files/sample.jpg")
               :mtype "image/jpeg"
               :size 312043}
        submitted (atom nil)]
    (with-redefs [cf/flags test-flags
                  audit/submit (fn [_ event] (reset! submitted event) nil)]
      (let [out (th/multipart-command! {::th/type :upload-file-media-object
                                        ::rpc/profile-id (:id prof)
                                        :file-id (:id file)
                                        :is-local true
                                        :name "testfile"
                                        :content mfile})]
        (t/is (nil? (:error out))))
      (let [revn (:revn (th/db-exec-one! ["SELECT revn FROM file WHERE id = ?" (:id file)]))]
        (th/run-task! :file-gc {:file-id (:id file) :revn revn}))
      (let [event @submitted]
        (t/is (= "file-gc" (:name event)))
        (t/is (= "action" (:type event)))
        (t/is (= "cron-task" (-> event :context :triggered-by)))
        (t/is (= (:id file) (-> event :props :file-id)))))
    (with-redefs [cf/flags test-flags]
      (append-row! "file-gc" {:file-id (:id file)})
      (th/run-task! :audit-log-consumer-storage-usage {})
      (t/is (some? (:dirty-at (usage-row team-id)))))))
