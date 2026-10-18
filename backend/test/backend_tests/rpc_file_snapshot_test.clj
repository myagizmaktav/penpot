;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns backend-tests.rpc-file-snapshot-test
  (:require
   [app.common.features :as cfeat]
   [app.common.pprint :as pp]
   [app.common.thumbnails :as thc]
   [app.common.time :as ct]
   [app.common.types.shape :as cts]
   [app.common.uuid :as uuid]
   [app.config :as cf]
   [app.db :as db]
   [app.db.sql :as sql]
   [app.http :as http]
   [app.rpc :as-alias rpc]
   [app.storage :as sto]
   [backend-tests.helpers :as th]
   [clojure.test :as t]
   [cuerdas.core :as str]))

(t/use-fixtures :once th/state-init)
(t/use-fixtures :each th/database-reset)

(defn- update-file!
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
    ;; (th/print-result! out)
    (t/is (nil? (:error out)))
    (:result out)))

(t/deftest snapshots-crud
  (let [profile (th/create-profile* 1 {:is-active true})
        team-id (:default-team-id profile)
        proj-id (:default-project-id profile)

        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id proj-id
                                    :is-shared false})
        snapshot-id (volatile! nil)]

    (t/testing "create snapshot"
      (let [params {::th/type :create-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :file-id (:id file)
                    :label "label1"}
            out    (th/command! params)]
        ;; (th/print-result! out)

        (t/is (nil? (:error out)))
        (let [result (:result out)]
          (t/is (= "label1" (:label result)))
          (t/is (uuid? (:id result)))
          (vswap! snapshot-id (constantly (:id result))))))

    (t/testing "list snapshots"
      (let [params {::th/type :get-file-snapshots
                    ::rpc/profile-id (:id profile)
                    :file-id (:id file)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (let [[row :as result] (:result out)]
          (t/is (= 1 (count result)))
          (t/is (= "label1" (:label row)))
          (t/is (uuid? (:id row)))
          (t/is (= @snapshot-id (:id row)))
          (t/is (= 0 (:revn row)))
          (t/is (= (:id profile) (:profile-id row))))))

    (t/testing "update snapshot with file-id"
      (let [params {::th/type :update-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :file-id (:id file)
                    :id @snapshot-id
                    :label "label1-updated"}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))
        (t/is (= "label1-updated"
                 (:label (th/db-get :file-change {:id @snapshot-id}))))))

    (t/testing "restore snapshot"
      (let [params {::th/type :restore-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :file-id (:id file)
                    :id @snapshot-id}
            out    (th/command! params)]

        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (nil? (:result out))))

      (let [[row1 row2 :as rows]
            (th/db-query :file-change
                         {:file-id (:id file)}
                         {:order-by [:created-at]})]

        (t/is (= 2 (count rows)))
        (t/is (= "user" (:created-by row1)))
        (t/is (= "system" (:created-by row2)))))

    (t/testing "delete snapshot"
      (let [[row1 row2 :as rows]
            (th/db-query :file-change
                         {:file-id (:id file)}
                         {:order-by [:created-at]})]

        (t/testing "delete user created snapshot"
          (let [params {::th/type :delete-file-snapshot
                        ::rpc/profile-id (:id profile)
                        :file-id (:id file)
                        :id (:id row1)}
                out    (th/command! params)]

            ;; (th/print-result! out)
            (t/is (nil? (:error out)))
            (t/is (true? (:result out)))))

        (t/testing "delete system created snapshot"
          (let [params {::th/type :delete-file-snapshot
                        ::rpc/profile-id (:id profile)
                        :file-id (:id file)
                        :id (:id row2)}
                out    (th/command! params)]

            ;; (th/print-result! out)
            (let [error (:error out)
                  data  (ex-data error)]
              (t/is (th/ex-info? error))
              (t/is (= (:type data) :validation))
              (t/is (= (:code data) :system-snapshots-cant-be-deleted)))))

        ;; this will run pending task triggered by deleting user snapshot
        (th/run-pending-tasks!)

        (binding [ct/*clock* (ct/fixed-clock (ct/in-future {:days 8}))]
          (let [res (th/run-task! :objects-gc {})]
            ;; delete 2 snapshots and 2 file data entries
            (t/is (= 4 (:processed res)))))))))

(t/deftest snapshots-locking
  (let [profile-1 (th/create-profile* 1 {:is-active true})
        profile-2 (th/create-profile* 2 {:is-active true})

        team
        (th/create-team* 1 {:profile-id (:id profile-1)})

        project
        (th/create-project* 1 {:profile-id (:id profile-1)
                               :team-id (:id team)})

        file
        (th/create-file* 1 {:profile-id (:id profile-1)
                            :project-id (:id project)
                            :is-shared false})

        snapshot
        (let [params {::th/type :create-file-snapshot
                      ::rpc/profile-id (:id profile-1)
                      :file-id (:id file)
                      :label "label1"}
              out    (th/command! params)]
          ;; (th/print-result! out)

          (t/is (nil? (:error out)))
          (:result out))]

    ;; Add the secont profile to the team
    (th/create-team-role* {:team-id (:id team)
                           :profile-id (:id profile-2)
                           :role :admin})

    (t/testing "lock snapshot"
      (let [params {::th/type :lock-file-snapshot
                    ::rpc/profile-id (:id profile-1)
                    :file-id (:id file)
                    :id (:id snapshot)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))

        (let [snapshot (th/db-get :file-change {:id (:id snapshot)} {::db/remove-deleted false})]
          (t/is (= (:id profile-1) (:locked-by snapshot))))))

    (t/testing "delete locked snapshot"
      (let [params {::th/type :delete-file-snapshot
                    ::rpc/profile-id (:id profile-2)
                    :file-id (:id file)
                    :id (:id snapshot)}
            out    (th/command! params)]

        ;; (th/print-result! out)
        (let [error (:error out)
              data  (ex-data error)]
          (t/is (th/ex-info? error))
          (t/is (= (:type data) :validation))
          (t/is (= (:code data) :snapshot-is-locked)))))

    (t/testing "unlock snapshot"
      (let [params {::th/type :unlock-file-snapshot
                    ::rpc/profile-id (:id profile-1)
                    :file-id (:id file)
                    :id (:id snapshot)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))

        (let [snapshot (th/db-get :file-change {:id (:id snapshot)})]
          (t/is (= nil (:locked-by snapshot))))))

    (t/testing "delete locked snapshot"
      (let [params {::th/type :delete-file-snapshot
                    ::rpc/profile-id (:id profile-2)
                    :file-id (:id file)
                    :id (:id snapshot)}
            out    (th/command! params)]

        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))))))

(t/deftest snapshots-crud-without-file-id
  ;; The snapshot RPC commands accept an optional `file-id` parameter (used
  ;; for partition pruning on the `file_change` table). Commands must keep
  ;; working without it, as older clients do not send it.
  (let [profile (th/create-profile* 1 {:is-active true})
        team-id (:default-team-id profile)
        proj-id (:default-project-id profile)

        file    (th/create-file* 1 {:profile-id (:id profile)
                                    :project-id proj-id
                                    :is-shared false})

        snapshot
        (let [params {::th/type :create-file-snapshot
                      ::rpc/profile-id (:id profile)
                      :file-id (:id file)
                      :label "label1"}
              out    (th/command! params)]
          ;; (th/print-result! out)

          (t/is (nil? (:error out)))
          (:result out))]

    (t/testing "update snapshot without file-id"
      (let [params {::th/type :update-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :id (:id snapshot)
                    :label "label2"}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))
        (t/is (= "label2"
                 (:label (th/db-get :file-change {:id (:id snapshot)}))))))

    (t/testing "lock snapshot without file-id"
      (let [params {::th/type :lock-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :id (:id snapshot)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))
        (t/is (= (:id profile)
                 (:locked-by (th/db-get :file-change {:id (:id snapshot)}))))))

    (t/testing "unlock snapshot without file-id"
      (let [params {::th/type :unlock-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :id (:id snapshot)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))
        (t/is (nil? (:locked-by (th/db-get :file-change {:id (:id snapshot)}))))))

    (t/testing "delete snapshot without file-id"
      (let [params {::th/type :delete-file-snapshot
                    ::rpc/profile-id (:id profile)
                    :id (:id snapshot)}
            out    (th/command! params)]
        ;; (th/print-result! out)
        (t/is (nil? (:error out)))
        (t/is (true? (:result out)))
        (t/is (some? (:deleted-at (th/db-get :file-change {:id (:id snapshot)}
                                             {::db/remove-deleted false})))))

      ;; process the delete-object task enqueued by the snapshot delete
      (th/run-pending-tasks!))))

(t/deftest snapshots-reject-a-file-id-that-does-not-match
  ;; The snapshot commands filter by `file_id` so that, once `file_change` is
  ;; hash-partitioned by it, the lookup prunes to a single partition. Sending
  ;; a `file-id` of another file must not find nor touch the snapshot.
  (let [profile  (th/create-profile* 1 {:is-active true})
        file-a   (th/create-file* 1 {:profile-id (:id profile)
                                     :project-id (:default-project-id profile)
                                     :is-shared false})
        file-b   (th/create-file* 2 {:profile-id (:id profile)
                                     :project-id (:default-project-id profile)
                                     :is-shared false})

        snapshot (-> (th/command! {::th/type :create-file-snapshot
                                   ::rpc/profile-id (:id profile)
                                   :file-id (:id file-a)
                                   :label "label1"})
                     :result)]

    (doseq [type [:update-file-snapshot :lock-file-snapshot
                  :unlock-file-snapshot :delete-file-snapshot]]
      (let [params (merge {::th/type type
                           ::rpc/profile-id (:id profile)
                           :file-id (:id file-b)
                           :id (:id snapshot)}
                          (when (= type :update-file-snapshot)
                            {:label "label2"}))
            out    (th/command! params)]

        (t/is (th/ex-info? (:error out)))
        (t/is (= :not-found (:type (ex-data (:error out)))))
        (t/is (= :snapshot-not-found (:code (ex-data (:error out)))))
        (t/is (= "label1"
                 (:label (th/db-get :file-change {:id (:id snapshot)}
                                    {::db/remove-deleted false})))
              "the snapshot of file-a stays intact")))))
