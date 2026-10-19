;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC Sucursal en España SL

(ns common-tests.files-migrations-0027-test
  "Decode-coverage probe for the planned 0027 migration
  (see https://github.com/penpot/penpot/issues/12433).

  Production files fail schema validation with unconverted types
  (`:type \"circle\"` instead of `:circle`, ids as text, map keys as
  text/keyword instead of uuid) and with nil pages. Before writing
  the migration, these characterization tests pin down what the
  schema `decode` (json-transformer) already fixes on its own and
  what still needs targeted repair clauses. They assert observed
  behavior, not fixed behavior."
  (:require
   [app.common.schema :as sm]
   [app.common.types.file :as ctf]
   [app.common.types.shape :as cts]
   [app.common.uuid :as uuid]
   [clojure.test :as t]))

(defn- make-rect-shape
  [shape-id]
  (cts/setup-shape {:id shape-id
                    :name "rect"
                    :type :rect
                    :x 0 :y 0 :width 10 :height 10
                    :parent-id uuid/zero
                    :frame-id uuid/zero}))

(defn- make-root-shape
  [shape-id]
  (cts/setup-shape {:id uuid/zero
                    :name "root"
                    :type :frame
                    :parent-id uuid/zero
                    :frame-id uuid/zero
                    :shapes [shape-id]}))

(defn- make-data
  "Minimal file data with a single page holding a single rect shape."
  [page-id shape]
  (let [shape-id (:id shape)]
    {:pages [page-id]
     :pages-index
     {page-id {:id page-id
               :name "Page 1"
               :objects {uuid/zero (make-root-shape shape-id)
                         shape-id shape}}}}))

(defn- decode-data
  [data]
  (sm/decode ctf/schema:data data (sm/json-transformer)))

(t/deftest decode-coverage-string-shape-type
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (make-data page-id (assoc (make-rect-shape shape-id) :type "rect"))
        decoded  (decode-data data)]
    (t/is (= :rect (get-in decoded [:pages-index page-id :objects shape-id :type]))
          "decode converts string shape type to keyword")))

(t/deftest decode-coverage-string-uuid-fields
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        shape    (-> (make-rect-shape shape-id)
                     (assoc :id (str shape-id))
                     (assoc :parent-id (str uuid/zero))
                     (assoc :frame-id (str uuid/zero)))
        data     (make-data page-id shape)
        decoded  (decode-data data)
        shape'   (get-in decoded [:pages-index page-id :objects shape-id])]
    (t/is (uuid? (:id shape')) "decode converts string shape id to uuid")
    (t/is (uuid? (:parent-id shape')) "decode converts string parent id to uuid")
    (t/is (uuid? (:frame-id shape')) "decode converts string frame id to uuid")))

(t/deftest decode-coverage-string-page-and-map-keys
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (make-data page-id (make-rect-shape shape-id))
        data     (-> data
                     (update :pages (fn [pages] (mapv str pages)))
                     (update :pages-index
                             (fn [pages-index]
                               (into {}
                                     (map (fn [[pid page]]
                                            [(str pid)
                                             (update page :objects
                                                     (fn [objects]
                                                       (into {}
                                                             (map (fn [[id shape]]
                                                                    [(keyword (str id)) shape]))
                                                             objects)))]))
                                     pages-index))))
        decoded  (decode-data data)]
    (t/is (= [page-id] (:pages decoded)) "decode converts string page refs to uuid")
    (t/is (contains? (:pages-index decoded) page-id) "decode converts string page keys to uuid")
    (t/is (contains? (get-in decoded [:pages-index page-id :objects]) shape-id)
          "decode converts keyword object keys to uuid")))

(t/deftest decode-coverage-nil-pages
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (-> (make-data page-id (make-rect-shape shape-id))
                     (update :pages conj nil)
                     (assoc-in [:pages-index nil]
                               {:id nil :name "ghost" :objects {}}))
        decoded  (decode-data data)]
    (t/is (some nil? (:pages decoded)) "decode leaves nil page entries alone")
    (t/is (contains? (:pages-index decoded) nil) "decode leaves nil page keys alone")))

(t/deftest decode-coverage-healthy-file-untouched
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (make-data page-id (make-rect-shape shape-id))
        _        (ctf/check-file-data (assoc data :id (uuid/next)))
        decoded  (decode-data data)
        shape'   (get-in decoded [:pages-index page-id :objects shape-id])]
    ;; NOTE: no whole-map equality here: decode materializes domain
    ;; records (Shape, Point, ...), which is the normal in-memory
    ;; representation (setup-shape builds the same records).
    (t/is (= (:type (get-in data [:pages-index page-id :objects shape-id]))
             (:type shape'))
          "decode preserves shape type")
    (t/is (= #{page-id} (set (:pages decoded))) "decode preserves page refs")
    (t/is (contains? (:pages-index decoded) page-id) "decode preserves page keys")
    (t/is (some? (ctf/check-file-data (assoc decoded :id (uuid/next))))
          "decoded healthy data stays valid")))
