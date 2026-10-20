;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns app.tasks.team-storage
  "Background recompute of the cached per-team media storage usage
  (`team_storage_usage`) that the media-storage-bytes-per-team quota
  reads instead of scanning `storage_object` on every upload.

  The audit-log storage-usage consumer marks dirty the teams whose
  media may have changed; this job recomputes a bounded number of the
  oldest dirty teams per run with the quota query, then sweeps the
  teams that were never computed (lazy backfill, no bulk load). It
  never holds row locks across the heavy query: marking dirty and
  writing the total are millisecond statements, so a mark landing
  mid-recompute only waits an instant."
  (:require
   [app.common.logging :as l]
   [app.common.schema :as sm]
   [app.db :as db]
   [app.jobs :as jobs]
   [app.metrics :as mtx]
   [app.rpc.quotes :as quotes]
   [integrant.core :as ig]))

(def ^:private sql:get-dirty-teams
  "SELECT team_id
     FROM team_storage_usage
    WHERE dirty_at IS NOT NULL
    ORDER BY dirty_at ASC
    LIMIT ?")

(def ^:private sql:get-never-computed-teams
  "SELECT t.id AS team_id
     FROM team AS t
     LEFT JOIN team_storage_usage AS u ON (u.team_id = t.id)
    WHERE t.deleted_at IS NULL
      AND u.team_id IS NULL
    ORDER BY t.created_at ASC
    LIMIT ?")

(def ^:private sql:upsert-team-storage-usage
  "INSERT INTO team_storage_usage AS tsu (team_id, total_bytes, dirty_at, updated_at)
   VALUES (?, ?, NULL, now())
   ON CONFLICT (team_id) DO UPDATE
      SET total_bytes = EXCLUDED.total_bytes,
          dirty_at = NULL,
          updated_at = now()")

(defn- recompute-team!
  "Recompute one team: slow read, fast write. A dirty mark landing
  mid-recompute waits only for the millisecond write; if it lands
  after the read the counter stays slightly stale until the next
  event marks the team again (accepted: abuse prevention, not a hard
  limit)."
  [{:keys [::db/conn]} team-id]
  (let [total (quotes/get-live-media-storage-bytes conn team-id)]
    (db/exec-one! conn [sql:upsert-team-storage-usage team-id total])
    total))

(defn execute-recompute-team-storage
  "Plain job handler: recompute the oldest dirty teams, then sweep
  never-computed teams with the leftover capacity. Bounded per run so
  the load stays predictable."
  ([cfg] (execute-recompute-team-storage cfg {}))
  ([cfg params]
   (let [chunk-size (or (:chunk-size params)
                        (::chunk-size cfg)
                        10)
         dirty      (mapv :team-id (db/exec! cfg [sql:get-dirty-teams chunk-size]))
         leftover   (- chunk-size (count dirty))
         fresh      (if (pos? leftover)
                      (mapv :team-id (db/exec! cfg [sql:get-never-computed-teams leftover]))
                      [])
         teams      (concat dirty fresh)]
     (reduce (fn [total team-id]
               (try
                 (db/tx-run! cfg
                             (fn [tx-cfg]
                               (let [bytes (recompute-team! tx-cfg team-id)]
                                 (l/trc :hint "team storage recomputed"
                                        :team-id (str team-id)
                                        :bytes bytes))))
                 (jobs/heartbeat cfg)
                 (inc total)
                 (catch Throwable cause
                   (l/err :hint "error recomputing team storage, skipping it"
                          :team-id (str team-id)
                          :cause cause)
                   total)))
             0
             teams))))

(def schema:recompute-team-storage-params
  "Optional :chunk-size caps the teams recomputed per run (dirty first,
  then never-computed)."
  [:map {:closed true}
   [:chunk-size {:optional true} ::sm/int]])

(defmethod ig/assert-key ::job-def
  [_ params]
  (assert (db/pool? (::db/pool params)) "expected a valid database pool")
  (assert (mtx/metrics? (::mtx/metrics params)) "expected valid metrics instance"))

(defmethod ig/expand-key ::job-def
  [k v]
  {k (merge {::chunk-size 10} v)})

(defmethod ig/init-key ::job-def
  [_ cfg]
  {::jobs/name      :recompute-team-storage
   ::jobs/schema    schema:recompute-team-storage-params
   ::jobs/decoder   (sm/decoder schema:recompute-team-storage-params sm/json-transformer)
   ::jobs/validator (sm/validator schema:recompute-team-storage-params)
   ::jobs/handler
   (fn [_context params]
     (execute-recompute-team-storage cfg params))})
