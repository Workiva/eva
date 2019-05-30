;; Copyright 2015-2019 Workiva Inc.
;; 
;; Licensed under the Eclipse Public License 1.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;; 
;;      http://opensource.org/licenses/eclipse-1.0.php
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns eva.v2.database.index
  (:require [eva.core :as core
             :refer [AdvanceIndex entry->datoms safe-advance-index ->root
                     safe-advance-index-better
                     select-datoms
                     multi-select-datoms-ordered
                     packed-datoms
                     packed-ref-type-datoms
                     packed-non-byte-datoms wildcard?]]
            [eva.datom :as d :refer [pack unpack]]
            [eva.entity-id :as entity-id]
            [eva.comparators :refer [index-cmp full-proj-cmp]]
            [eva.v2.datastructures.bbtree :refer [backed-sorted-set-by
                                                  open-set
                                                  open-writable-set
                                                  between
                                                  subranges
                                                  persist!
                                                  subrange
                                                  remove-interval!
                                                  remove-interval
                                                  storage-id
                                                  make-editable!]]
            [eva.v2.datastructures.vector :refer [read-range]]
            [eva.utils :refer [ensure-avl-sorted-set-by fill one with-retries]]
            [eva.datastructures.utils.comparators :refer [LOWER UPPER]]
            [eva.datastructures.utils.interval :refer [open-interval]]
            [eva.defaults :as defaults]
            [eva.error :refer [insist raise]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :refer [info debug trace spy error warn warnf]]
            [eva.utils.delay-queue :as d-q]
            [ichnaie.core :refer [tracing]]
            [plumbing.core :as pc]
            [schema.core :as s]
            [eva.attribute :as attr])
  (:import (eva Datom Database)))

(defn ref-datom? [^Database db ^Datom datom] (attr/ref-attr? db (.a datom)))

(defn filter-datoms-for-index [db index-name datoms]
  (cond (contains? #{:vaet :vaeth} index-name)
        (filter (partial ref-datom? db) datoms)
        :else
        datoms))

(defn historical? [index] (:hist? index))

(defn apply-op! [index [op datom-vec :as d]]
  (case op
    :conj (conj! index datom-vec)
    :remove-interval (remove-interval! index datom-vec)
    (raise :eva.indexes/unknown-op
           (format "Unknown op %s passed to conj-datom!" op)
           {:op op, ::sanex/sanitary? true})))

(s/defrecord IndexRoot [index tx-num])

(defmethod print-method IndexRoot [root ^java.io.Writer v]
  (.write v (format "#index-root[%s]" (:tx-num root))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils for updating with ref-attr-set instead of database as a whole

(def ^:dynamic *db-part-eid* 0)
(def ^:dynamic *db-inst-attr-eid* 20)
(def ^:dynamic *db-ref-type-eid* 32)
(def ^:dynamic *db-byte-type-eid* 27)
(def ^:dynamic *db-vt-attr-eid* 4)

(defn datoms->new-type-attrs
  [datoms type-eid]
  (let [new-attr-eids
        (map :v (select-datoms datoms [:eavt *db-part-eid* type-eid]))
        new-ref-attrs
        (into #{}
              (comp (map first) (remove nil?) (map :e))
              (->> new-attr-eids
                   (map #(do [% *db-vt-attr-eid* type-eid]))
                   (cons :eavt)
                   (multi-select-datoms-ordered datoms)))]
    new-ref-attrs))

(defn update-attr-set [attr-set datoms type-eid]
  (clojure.set/union attr-set (datoms->new-type-attrs datoms type-eid)))

(defn db->type-attr-eids [db type-kw]
  (let [attrs (:attrs db)]
    (into #{}
          (comp (filter (fn [[_ attr-data]]
                          (= type-kw (:value-type attr-data))))
                (map first))
          attrs)))

(defn filter-ref-datoms [index-name ref-attr-eids datoms]
  (cond (contains? #{:vaet :vaeth} index-name)
        (filter (fn [d] (contains? ref-attr-eids (:a d))) datoms)
        :else datoms))

(defn remove-byte-datoms [index-name byte-attr-eids datoms]
  (cond (contains? #{:avet :aveth} index-name)
        (remove (fn [d] (contains? byte-attr-eids (:a d))) datoms)
        :else datoms))

(defn ->interval [c0 c1 c2]
  (open-interval [c0 c1 c2 LOWER] [c0 c1 c2 UPPER]))

(defn unpack-eid [e] (bit-clear e 62))

(defn packed-datom->op [index-name [c0 c1 c2 tx-eid]] ;; order: peid a v tx-eid
  (case index-name
    :aevt
    (if (entity-id/added? c0)
      [:conj [c1 c0 c2 tx-eid]]
      [:remove-interval (->interval c1 (unpack-eid c0) c2)])
    :avet
    (if (entity-id/added? c0)
      [:conj [c1 c2 c0 tx-eid]]
      [:remove-interval (->interval c1 c2 (unpack-eid c0))])
    :eavt
    (if (entity-id/added? c0)
      [:conj [c0 c1 c2 tx-eid]]
      [:remove-interval (->interval (unpack-eid c0) c1 c2)])
    :vaet
    (if (entity-id/added? c0)
      [:conj [c2 c1 c0 tx-eid]]
      [:remove-interval (->interval c2 c1 (unpack-eid c0))])
    :aevth [:conj [c1 c0 c2 tx-eid]]
    :aveth [:conj [c1 c2 c0 tx-eid]]
    :eavth [:conj [c0 c1 c2 tx-eid]]
    :vaeth [:conj [c2 c0 c1 tx-eid]]))

(defn log->index-ops [index-name tx-log start-tx target-tx-entry]
  (let [target-tx-num (:tx-num target-tx-entry)
        log-entries  (concat (when (> target-tx-num start-tx)
                               @(read-range tx-log start-tx target-tx-num)) [target-tx-entry])
        log-selector-fn (case index-name
                          (:avet :aveth) packed-non-byte-datoms
                          (:vaet :vaeth) packed-ref-type-datoms
                          packed-datoms)]
    (sequence (comp (mapcat log-selector-fn)
                    (map (partial packed-datom->op index-name)))
              log-entries)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn pad-components [components]
  (assert (<= 0 (count components) 4))
  (take 4 (concat components (repeat core/wildcard))))

(defn expand-components
  "Expands the given 4-tuple of components to an interval for range selection
   from the indexes. If *any* component is nil, returns nil."
  [[c0 c1 c2 c3 :as components]]
  (when-not (some nil? components)
    [[(if (wildcard? c0) LOWER c0)
      (if (wildcard? c1) LOWER c1)
      (if (wildcard? c2) LOWER c2)
      (if (wildcard? c3) LOWER c3)]
     [(if (wildcard? c0) UPPER c0)
      (if (wildcard? c1) UPPER c1)
      (if (wildcard? c2) UPPER c2)
      (if (wildcard? c3) UPPER c3)]]))

(s/defrecord Index [name hist? idx tx]
  core/SelectDatoms
  (select-datoms [_ [index-name & components]]
    (insist (= name index-name)
            (format "select-datoms expected index %s but was called against %s" index-name name))
    (if-let [[lower upper] (-> components pad-components expand-components)]
      (with-retries (defaults/read-index-retry-strategy)
        (format "retrying read on select-datoms %s" index-name)
        (map (partial unpack name)
             (between idx lower upper)))
      ()))

  core/MultiSelectDatoms
  (multi-select-datoms [_ [index-name & component-colls]]
    (insist (= name index-name)
            (format "multi-select-datoms expected index %s but was called against %s" index-name name))
    (tracing "eva.Index/multi-select-datoms"
      (with-retries (defaults/read-index-retry-strategy)
        (format "retrying read on multi-select-datoms %s %s" index-name component-colls)
        (let [filled (map (fn [c] [(fill LOWER c) (fill UPPER c)]) component-colls)
              range->selection (subranges idx filled)]
          (zipmap component-colls
                  (map (comp #(map (partial unpack name) %) range->selection)
                       filled))))))

  (multi-select-datoms-ordered [_ [index-name & component-colls]]
    (insist (= name index-name)
            (format "multi-select-datoms-ordered expected index %s but was called against %s" index-name name))
    (tracing "eva.Index/multi-select-datoms-ordered"
      (with-retries (defaults/read-index-retry-strategy)
        (format "retrying read on multi-select-datoms-ordered %s %s" index-name component-colls)
        (let [ranges (map (comp expand-components pad-components) component-colls)
              groups (group-by nil? (distinct ranges))
              range->selection (-> (when (contains? groups false)
                                     (subranges idx (get groups false)))
                                   (merge (zipmap (get groups true) (repeat ()))))]
          (->> ranges
               (sequence (comp (map range->selection)
                               ;; TODO: for queries with the same range repeated many times,
                               ;;       the following line of work will be duplicated quite a bit.
                               (map #(map (partial unpack name) %))))
               (doall))))))
  AdvanceIndex
  (initialize-index [this init-datoms]
    (insist (< tx 0) "Index is already initialized!")
    (try (with-retries (defaults/init-fill-index-retry-strategy)
           "caught exception while initializing index. Retrying."
           (let [datoms (->> init-datoms (map #(pack % name)))
                 new-index (persist! (persistent! (reduce apply-op! (transient idx) datoms)))]
             (merge this {:idx new-index, :tx 0})))
      (catch Exception e
        (error "Unhandled exception while initializing index.")
        (throw e))))
  (safe-advance-index-better [this ref-attr-eids byte-attr-eids tx-log-entry]
    (insist (= tx (dec (:tx-num tx-log-entry)))
            (format "The provided tx-log-entry is not the successor to the current index state. %s %s"
                    tx (-> tx-log-entry :tx-num dec)))
    (let [datoms (->> (entry->datoms tx-log-entry)
                      (filter-ref-datoms name ref-attr-eids)
                      (remove-byte-datoms name byte-attr-eids)
                      (map #(pack % name)))
          new-index (persistent! (reduce apply-op! (transient idx) datoms))
          new-tx (inc tx)]
      (merge this {:idx new-index :tx new-tx})))
  (batch-advance-index
   [this tx-log target-tx-entry]
   (if (> (:tx-num target-tx-entry) tx)
     (let [tx-num (:tx-num target-tx-entry)
           start-tx (inc tx)
           ops (log->index-ops name tx-log start-tx target-tx-entry)
           new-idx (persistent! (reduce apply-op! (transient idx) ops))]
       (merge this {:idx new-idx :tx tx-num}))
     (do (warnf "batch-advance-index ignoring out-of-order request to advance to %s from %s"
                (:tx-num target-tx-entry) tx)
         this)))
  (->root [this] (strict-map->IndexRoot {:index (storage-id idx) :tx-num tx})))

(defn create-index
  [store index-name init-datoms]
  (try (with-retries (defaults/create-index-retry-strategy)
         "caught exception while constructing named index. Retrying."
         (let [empty-index (backed-sorted-set-by (index-cmp index-name) store)
               datoms (->> init-datoms (map #(pack % index-name)))
               new-index (persist! (persistent! (reduce apply-op! (transient empty-index) datoms)))]
           (map->Index {:name  index-name
                        :hist? (= (last (str index-name)) \h)
                        :idx   new-index
                        :tx    0})))
       (catch Exception e
         (error "unhandled exception while creating named index:" index-name)
         (throw e))))

(defn open-index
  [store index-name root]
  (try (with-retries (defaults/open-index-retry-strategy)
         (format "caught exception when opening readable index: %s. Retrying." index-name)
         (strict-map->Index {:name  index-name
                             :hist? (= (last (str index-name)) \h)
                             :idx   (open-set store (:index root))
                             :tx    (:tx-num root)}))
       (catch Exception e
         (error "Unhandled exception while opening readable index:" index-name)
         (throw e))))

(defn open-writable-index
  [store index-name root]
  (try (with-retries (defaults/open-index-retry-strategy)
         (format "caught exception when opening readable index: %s. Retrying." index-name)
         (strict-map->Index {:name  index-name
                             :hist? (= (last (str index-name)) \h)
                             :idx   (open-writable-set store (:index root))
                             :tx    (:tx-num root)}))
       (catch Exception e
         (error "Unhandled exception while opening readable index:" index-name)
         (throw e))))

(defn open-indexes
  [store index-roots]
  (pc/for-map [[index-name root] index-roots]
              index-name
              (d-q/delay-queue (open-index store index-name root))))

(defn open-writable-indexes
  [store index-roots]
  (pc/for-map [[index-name root] index-roots]
              index-name
              (open-writable-index store index-name root)))

(defn flush-index [index]
  (debug "Flushing index:" (:name @index))
  (d-q/delay-queue (update @index :idx persist!)))

(defn update-all-indexes [log index-roots target-tx])
