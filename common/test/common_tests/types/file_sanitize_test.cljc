;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns common-tests.types.file-sanitize-test
  "Pins the behavior of `ctf/sanitize-file-data`: it coerces the
  unconverted types seen in production (strings where the schema
  expects keywords or uuids) and drops nil page entries, while
  leaving genuinely unknown values untouched so downstream
  validation still fails loudly."
  (:require
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

(t/deftest sanitize-coerces-unconverted-types
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        shape    (-> (make-rect-shape shape-id)
                     (assoc :type "rect")
                     (assoc :id (str shape-id))
                     (assoc :parent-id (str uuid/zero))
                     (assoc :frame-id (str uuid/zero)))
        data     (-> (make-data page-id shape)
                     (update :pages (fn [pages] (mapv str pages)))
                     (update :pages-index
                             (fn [pages-index]
                               (into {}
                                     (map (fn [[pid page]]
                                            [(str pid)
                                             (update page :objects
                                                     (fn [objects]
                                                       (into {}
                                                             (map (fn [[id s]]
                                                                    [(keyword (str id)) s]))
                                                             objects)))]))
                                     pages-index))))
        data'    (ctf/sanitize-file-data data)
        shape'   (get-in data' [:pages-index page-id :objects shape-id])]
    (t/is (= [page-id] (:pages data')) "string page refs become uuids")
    (t/is (contains? (:pages-index data') page-id) "string page keys become uuids")
    (t/is (= :rect (:type shape')) "string shape type becomes a keyword")
    (t/is (uuid? (:id shape')) "string shape id becomes a uuid")))

(t/deftest sanitize-drops-nil-pages
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (-> (make-data page-id (make-rect-shape shape-id))
                     (update :pages conj nil)
                     (assoc-in [:pages-index nil] {:id nil :name "ghost" :objects {}}))
        data'    (ctf/sanitize-file-data data)]
    (t/is (= [page-id] (:pages data')) "nil page entries are dropped")
    (t/is (not (contains? (:pages-index data') nil)) "nil page keys are dropped")))

(t/deftest sanitize-leaves-unknown-values-untouched
  (let [page-id  (uuid/next)
        shape-id (uuid/next)
        data     (make-data page-id
                            (assoc (make-rect-shape shape-id) :type "triangle"))
        data'    (ctf/sanitize-file-data data)
        shape'   (get-in data' [:pages-index page-id :objects shape-id])]
    (t/is (= :triangle (:type shape'))
          "decode keywordizes the string, but the value stays out of schema")
    (t/is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                   (ctf/check-file-data data'))
          "downstream validation still fails loudly")
    (t/is (= data' (ctf/sanitize-file-data data'))
          "sanitize is idempotent")))
