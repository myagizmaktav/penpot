;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC Sucursal en España SL

(ns app.tasks.file-gc
  "A maintenance task that is responsible of: purge unused file media,
  clean unused object thumbnails and remove old file thumbnails.  The
  file is eligible to be garbage collected after some period of
  inactivity (the default threshold is 72h)."
  (:require
   [app.binfile.cleaner :as bfl]
   [app.binfile.common :as bfc]
   [app.common.exceptions :as ex]
   [app.common.files.changes :as cfc]
   [app.common.files.helpers :as cfh]
   [app.common.files.migrations :as fmg]
   [app.common.files.repair :as cfr]
   [app.common.files.validate :as cfv]
   [app.common.logging :as l]
   [app.common.thumbnails :as thc]
   [app.common.time :as ct]
   [app.common.types.components-list :as ctkl]
   [app.common.types.file :as ctf]
   [app.common.types.shape-tree :as ctt]
   [app.config :as cf]
   [app.db :as db]
   [app.features.fdata :as feat.fdata]
   [app.features.file-snapshots :as fsnap]
   [app.storage :as sto]
   [app.worker :as wrk]
   [integrant.core :as ig]))

(def ^:private sql:mark-file-media-object-deleted
  "UPDATE file_media_object
      SET deleted_at = ?
    WHERE file_id = ? AND id != ALL(?::uuid[])
   RETURNING id")

(def xf:collect-used-media
  (comp
   (map :data)
   (mapcat cfh/collect-used-media)))

(defn- clean-file-media
  "Performs the garbage collection of file media objects."
  [{:keys [::db/conn ::timestamp] :as cfg} {:keys [id] :as file}]
  (let [used-media
        (fsnap/reduce-snapshots cfg id xf:collect-used-media conj #{})

        used-media
        (into used-media xf:collect-used-media [file])

        used-media
        (db/create-array conn "uuid" used-media)

        unused-media
        (->> (db/exec! conn [sql:mark-file-media-object-deleted timestamp id used-media])
             (into #{} (map :id)))]

    (doseq [id unused-media]
      (l/trc :obj "media-object"
             :file-id (str id)
             :id (str id)))

    file))

(def ^:private sql:mark-file-object-thumbnails-deleted
  "UPDATE file_tagged_object_thumbnail
      SET deleted_at = ?
    WHERE file_id = ? AND object_id != ALL(?::text[])
   RETURNING object_id")

(defn- clean-file-object-thumbnails
  [{:keys [::db/conn ::timestamp]} {:keys [data] :as file}]
  (let [file-id (:id file)
        using   (->> (vals (:pages-index data))
                     (into #{} (comp
                                (mapcat (fn [{:keys [id objects]}]
                                          (->> (ctt/get-frames objects)
                                               (map #(assoc % :page-id id)))))
                                (mapcat (fn [{:keys [id page-id]}]
                                          (list
                                           (thc/fmt-object-id file-id page-id id "frame")
                                           (thc/fmt-object-id file-id page-id id "component")))))))

        ids    (into-array String using)
        unused (->> (db/exec! conn [sql:mark-file-object-thumbnails-deleted timestamp file-id ids])
                    (into #{} (map :object-id)))]

    (doseq [object-id unused]
      (l/trc :obj "object-thumbnail"
             :file-id (str file-id)
             :id object-id))

    file))

(def ^:private sql:mark-file-thumbnails-deleted
  "UPDATE file_thumbnail
      SET deleted_at = ?
    WHERE file_id = ? AND revn < ?
   RETURNING revn")

(defn- clean-file-thumbnails
  [{:keys [::db/conn ::timestamp]} {:keys [id revn] :as file}]
  (let [unused (->> (db/exec! conn [sql:mark-file-thumbnails-deleted timestamp id revn])
                    (into #{} (map :revn)))]

    (doseq [revn unused]
      (l/trc :obj "thumbnail"
             :file-id (str id)
             :revn revn))

    file))

(def ^:private sql:get-files-for-library
  "SELECT f.id
     FROM file AS f
     LEFT JOIN file_library_rel AS fl ON (fl.file_id = f.id)
    WHERE fl.library_file_id = ?
      AND f.deleted_at IS null
    ORDER BY f.modified_at ASC")

(defn- get-used-components
  "Given a file and a set of components marked for deletion, return a
  filtered set of component ids that are still un use"
  [components library-id {:keys [data]}]
  (filter #(ctf/used-in? data library-id % :component) components))

(defn- get-library-user-ids
  "Materialize the rows of the files using this one as a library with
  a plain query (no cursor), so no write transaction needs to stay
  open while their file data is loaded."
  [cfg library-id]
  (db/exec! cfg [sql:get-files-for-library library-id]))

(defn- find-used-components
  "Compute the set of deleted component ids still in use by library
  users. Every user file is loaded on its own connection, outside any
  write transaction. Skips the scan entirely when there is nothing
  deleted to collect."
  [cfg file deleted-components]
  (if (seq deleted-components)
    (let [file-id       (:id file)
          file-xform    (mapcat (partial get-used-components deleted-components file-id))
          library-xform (comp (map :id)
                              (map #(bfc/get-file cfg % :realize? true :read-only? true))
                              file-xform)]
      (->> (get-library-user-ids cfg file-id)
           (transduce library-xform conj #{})))
    #{}))

(defn- clean-deleted-components
  "Performs the garbage collection of unreferenced deleted components.
  Takes the set of component ids still used by library users and the
  deleted components, both computed beforehand outside any write
  transaction. Both stay valid under the revn guard: if the file
  changed in between, Tx2 aborts before using them."
  [used-remote deleted-components {:keys [id] :as file}]
  (let [file-id id
        file-xform
        (mapcat (partial get-used-components deleted-components file-id))

        used-local
        (into #{} file-xform [file])

        unused
        (transduce bfc/xf-map-id disj
                   (into #{} bfc/xf-map-id deleted-components)
                   (concat used-remote used-local))

        file
        (update file :data
                (fn [data]
                  (reduce (fn [data id]
                            (l/trc :obj "component"
                                   :file-id (str file-id)
                                   :id (str id))
                            (ctkl/delete-component data id))
                          data
                          unused)))]

    file))

(def ^:private sql:mark-deleted-data-fragments
  "UPDATE file_data
      SET deleted_at = ?
    WHERE file_id = ?
      AND id != ALL(?::uuid[])
      AND type = 'fragment'
      AND deleted_at IS NULL
   RETURNING id")

(def ^:private xf:collect-pointers
  (comp (map :data)
        (mapcat feat.fdata/get-used-pointer-ids)))

(defn- clean-fragments
  [{:keys [::db/conn ::timestamp]} {:keys [id] :as file}]
  (let [used   (into #{} xf:collect-pointers [file])
        unused (->> (db/exec! conn [sql:mark-deleted-data-fragments timestamp id
                                    (db/create-array conn "uuid" used)])
                    (into #{} bfc/xf-map-id))]

    (doseq [id unused]
      (l/trc :obj "fragment"
             :file-id (str id)
             :id (str id)))

    file))

(defn- clean-media
  [cfg used-remote deleted-components file]
  (let [file (->> file
                  (clean-deleted-components used-remote deleted-components)
                  (clean-file-media cfg)
                  (clean-file-thumbnails cfg)
                  (clean-file-object-thumbnails cfg))]
    (cfv/validate-file-schema! file)
    file))

;; FIXME: we should skip files that does not match the revn on the
;; props and add proper schema for this task props

(defn- repair-loop
  "Runs the semantic repair loop over an in-memory file. Repair is
  best-effort: when it throws or does not converge, the original file
  is returned untouched so the garbage collection still runs as
  before."
  [file libs]
  (try
    (loop [current file
           iteration 0]
      (let [errors (not-empty (cfv/validate-file current libs))]
        (cond
          (empty? errors)
          (do
            (when (pos? iteration)
              (l/trc :hint "file repaired"
                     :file-id (str (:id current))
                     :iterations iteration))
            {:file current :repaired? (pos? iteration)})

          (>= iteration 10)
          (do
            (l/wrn :hint "file still invalid after repair, cleaning as is"
                   :file-id (str (:id current))
                   :errors (count errors))
            {:file file})

          :else
          (do
            (l/trc :hint "repairing file"
                   :file-id (str (:id current))
                   :iteration iteration
                   :errors (count errors))
            (let [changes (cfr/repair-file current libs errors)]
              (recur (-> current
                         (update :revn inc)
                         (update :data cfc/process-changes changes))
                     (inc iteration)))))))
    (catch Throwable cause
      (l/wrn :hint "error on repairing file, cleaning as is"
             :file-id (str (:id file))
             :cause cause)
      {:file file})))

(defn- get-file
  "Reads the file holding the row lock without migrating, sanitizes
  first so legacy migrations do not choke on unconverted types, then
  migrates in memory. Never writes: the second transaction persists
  the snapshot."
  [cfg {:keys [file-id] :as props}]
  (when-let [file (bfc/get-file cfg file-id
                                :realize? true
                                :skip-locked? true
                                :lock-for-update? true
                                :migrate? false)]
    (let [libs  (delay (bfc/get-resolved-file-libraries cfg file))
          clean (-> file
                    (update :data ctf/sanitize-file-data)
                    (fmg/migrate-file libs))]
      ;; We should ensure that the scheduled file and the procesing file
      ;; has not changed since schedule, for this reason we check the
      ;; revn from props with the revn from retrieved file from database
      (when (or (nil? (:revn props)) (= (:revn props) (:revn clean)))
        clean))))

(defn- repair-and-persist
  "First transaction: read the file holding the row lock, run the
  semantic repair loop when validation reports errors, check the
  schema of the result and persist it when it was repaired. A file
  that is still schema-invalid after the repair fails here (instead
  of at the end of the second transaction), so it is never marked as
  cleaned. Returns the file to clean, or nil when the file cannot be
  read or repaired."
  [cfg props]
  (db/tx-run! cfg
              (fn [{:keys [::db/conn] :as cfg}]
                (let [cfg (-> cfg
                              (update ::sto/storage sto/configure conn)
                              (assoc ::timestamp (ct/now)))]
                  (if-let [file (try
                                  (get-file cfg props)
                                  (catch Throwable cause
                                    (ex/raise :type :internal
                                              :hint "file-gc read failed"
                                              :file-id (str (:file-id props))
                                              :phase :tx1-read
                                              :cause cause)))]
                    (try
                      ;; Libraries resolve lazily through the weak map;
                      ;; each one realizes on load with its own loader,
                      ;; so validation needs no ambient *load-fn*.
                      (let [libs (bfc/get-resolved-file-libraries cfg file)
                            {:keys [file repaired?]} (repair-loop (-> file bfl/clean-file) libs)]
                        (cfv/validate-file-schema! file)
                        (when repaired?
                          (bfc/update-file! cfg file))
                        file)
                      (catch Throwable cause
                        (l/err :hint "error on repairing file"
                               :file-id (str (:file-id props))
                               :cause cause)
                        nil))
                    (do
                      (l/dbg :hint "skip cleaning, criteria does not match"
                             :file-id (str (:file-id props)))
                      nil))))))


(def ^:private sql:lock-file-revn
  "SELECT f.revn FROM file AS f WHERE f.id = ? AND f.deleted_at IS NULL FOR UPDATE OF f SKIP LOCKED")

(defn- lock-file-revn
  "Take the row lock and return the current revn, or nil when the row
  is locked elsewhere or gone."
  [conn id]
  (:revn (db/exec-one! conn [sql:lock-file-revn id])))

(defn- clean-file
  "Second transaction: take the row lock, check nobody modified the
  file since the first transaction, and run the garbage collection on
  its snapshot instead of re-reading it. Takes the file returned by
  the first transaction, the set of component ids still in use by
  library users and the deleted components, both computed beforehand
  outside any write transaction."
  [{:keys [::db/conn] :as cfg} used-remote deleted-components {:keys [id revn] :as file}]
  (if (= revn (lock-file-revn conn id))
    (let [file (->> file
                    bfl/clean-file
                    (clean-media cfg used-remote deleted-components)
                    (clean-fragments cfg))
          file (assoc file :has-media-trimmed true)]
      (bfc/update-file! cfg file)
      true)

    (do
      (l/dbg :hint "skip cleaning, file changed since repair" :file-id (str id))
      false)))

(defn- clean-file-in-tx
  "Second transaction: run the garbage collection on the snapshot
  returned by the first transaction. Everything expensive (loading the
  files that use this one as a library) happens outside any write
  transaction; the write transaction itself only takes the row lock,
  checks nobody modified the file in the meantime, applies the marks
  and commits. Returns true when the file was cleaned."
  [cfg {:keys [data] :as file}]
  (let [deleted-components (ctkl/deleted-components-seq data)
        used-remote        (find-used-components cfg file deleted-components)]
    (db/tx-run! cfg
                (fn [{:keys [::db/conn] :as cfg}]
                  (let [cfg (-> cfg
                                (update ::sto/storage sto/configure conn)
                                (assoc ::timestamp (ct/now)))]
                    (clean-file cfg used-remote deleted-components file))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; HANDLER
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/assert-key ::handler
  [_ params]
  (assert (db/pool? (::db/pool params)) "expected a valid database pool")
  (assert (sto/valid-storage? (::sto/storage params)) "expected valid storage to be provided"))

(defmethod ig/init-key ::handler
  [_ cfg]
  (fn [{:keys [props] :as task}]
    (try
      (let [base-cfg   (assoc cfg ::db/rollback (:rollback? props))
            repaired   (try
                         (repair-and-persist base-cfg props)
                         (catch Throwable cause
                           (l/err :hint "error on repairing file"
                                  :file-id (str (:file-id props))
                                  :cause cause)
                           nil))
            processed? (when repaired
                         (try
                           (clean-file-in-tx base-cfg repaired)
                           (catch Throwable cause
                             (l/err :hint "error on cleaning file"
                                    :file-id (str (:file-id props))
                                    :cause cause)
                             nil)))]

        (when (and processed? (contains? cf/flags :tiered-file-data-storage))
          (wrk/submit! (-> cfg
                           (assoc ::wrk/task :offload-file-data)
                           (assoc ::wrk/params props)
                           (assoc ::wrk/priority 10)
                           (assoc ::wrk/delay 1000))))
        processed?)
      (catch Throwable cause
        (l/err :hint "error on cleaning file"
               :file-id (str (:file-id props))
               :cause cause)))))
