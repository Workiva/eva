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

(ns eva.v2.database.core
  (:require [eva.v2.database.index-manager :refer [get-indexes]]
            [eva.v2.datastructures.vector :refer [read-range lazy-read-range]]
            [eva.v2.database.log :refer [log-entry open-transaction-log set-log-count]]
            [eva.v2.database.lookup-refs :as lookup-refs]
            [eva.v2.database.overlay :as over]
            [eva.v2.transaction-pipeline.core :as transaction-pipeline]
            [eva.core :refer [entry->datoms safe-advance-index-better batch-advance-index] :as core]
            [eva.attribute :as attr]
            [eva.datom :as dat]
            [eva.db-update :refer [UpdatableDB safe-advance-db flush-overlay advance-db-to-tx advance-db* speculatively-advance-db]]
            [eva.entity-id :as entity-id :refer [->tx-num]]
            [eva.v2.database.history-snapshot :as hdb]
            [eva.defaults :as defaults]
            [eva.functions :refer [build-db-fn]]
            [eva.readers :as eva-reader]
            [eva.entity]
            [eva.query.datalog.protocols :as p]
            [eva.query.dialect.pull.core :as pull-query]
            [barometer.core :as metrics]
            [barometer.aspects :refer [timed]]
            [eva.utils :refer [with-retries one ensure-avl-sorted-set-by fill]]
            [recide.sanex :as sanex]
            [recide.core :as rc]
            [eva.utils.logging :refer [logged]]
            [map-experiments.smart-maps.bijection :refer [bijection]]
            [map-experiments.smart-maps.protocol :as smart-maps]
            [utiliva.core :refer [zip-to zip-from partition-map]]
            [eva.error :refer [raise insist] :as ee]
            [clojure.core.unify :as u]
            [morphe.core :as d]
            [ichnaie.core :refer [traced tracing]]
            [clojure.spec.alpha :as s]
            [plumbing.core :as pc])
  (:import (eva.entity_id EntityID)
           (clojure.lang MapEntry)))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::id uuid?)

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(def background-processed-idxs #{:eavt :aevt :avet :vaet})
(def resource-priorities (zipmap [:eavt :aevt :avet :vaet] (range)))
(defn resource-selector
  [tag->delay]
  (apply min-key
         (comp resource-priorities key)
         tag->delay))

(def ^:dynamic *db-ident-eid* 3)
(def ^:dynamic *db-fn-eid* 11)
(def ^:dynamic *install-part-eid* 19)
(def ^:dynamic *install-attr-eid* 20)
(def ^:dynamic *tx-partition-id* 1)

(def ^:dynamic *db-part-eid* 0)
(def ^:dynamic *db-ref-type-eid* 32)
(def ^:dynamic *db-byte-type-eid* 27)

(declare unique ref-datom? read-range-retry)
(declare resolve-partition)

;;;;;;;;;;;;
;; IDENTS ;;
;;;;;;;;;;;;

(defn ->idents
  "Creates a bijective map ident <--> eid"
  [indexes]
  (into (bijection)
        (map (juxt :v :e))
        (core/select-datoms indexes [:aevt *db-ident-eid*])))

;;;;;;;;;;;;;;;;
;; ATTRIBUTES ;;
;;;;;;;;;;;;;;;;

(defn attribute*
  "Builds an attribute entity from a raw query result on the database"
  [attr-datom-set idents]
  (let [e (:e (first attr-datom-set))
        inv (smart-maps/inverse idents)]
    (attr/map->Attribute
     (-> (into {}
               (map (juxt (comp attr/through-attr-bij #(get inv %) :a)
                          (comp #(get inv % %) :v)))
               attr-datom-set)
         (assoc (attr/through-attr-bij :db/id) e)))))

(defn dats->attrs
  [indexes idents attr-dats]
  (let [eids (map (comp list :v) attr-dats)
        attrs (core/multi-select-datoms-ordered indexes (cons :eavt eids))]
    (into (sorted-map)
          (comp (map #(attribute* % idents))
                (map (juxt :id identity)))
          attrs)))

(defn ->attrs
  "Creates a map attr-eid --> attribute-map"
  [indexes idents]
  (let [installed-attr-datoms
        (core/select-datoms indexes [:eavt *db-part-eid* (:db.install/attribute idents)])]
    (dats->attrs indexes idents installed-attr-datoms)))

;;;;;;;;;;;;;;;;
;; PARTITIONS ;;
;;;;;;;;;;;;;;;;

(defn dats->partitions
  [idents cur-count-parts part-dats]
  (into (bijection)
        (comp (map :v)
              (map (smart-maps/inverse idents))
              (zip-to (drop cur-count-parts (range))))
        (sort-by (juxt :tx :v) part-dats)))

(defn ->parts
  "Creates a map partition-eid --> partition-id (NOT EID)"
  [indexes idents]
  (->> (core/select-datoms indexes [:eavt *db-part-eid* (:db.install/partition idents)])
       (dats->partitions idents 0)))

;;;;;;;;;;;;
;; DB FNS ;;
;;;;;;;;;;;;

(defn dats->db-fns
  [idents fn-dats]
  (into {}
        (map (juxt :e (fn [fn-dat]
                        (let [maybe-ident (get (smart-maps/inverse idents) (:e fn-dat))
                              maybe-fn-name (when (some? maybe-ident) (subs (str maybe-ident) 1))]
                          (build-db-fn
                           maybe-fn-name
                           (:v fn-dat))))))
        fn-dats))

(defn ->db-fns
  "Constructs a map of db-fn-eid --> db-fn"
  [indexes idents]
  (let [installed-db-fn-datoms
        (core/select-datoms indexes [:aevt (:db/fn idents)])]
    (dats->db-fns idents installed-db-fn-datoms)))

(defn update-db-fns
  [db-fns idents fn-dats]
  (let [{retracted false, added true} (group-by dat/add? fn-dats)]
    (-> (transduce (map :e) dissoc db-fns retracted)
        (merge (dats->db-fns idents added)))))

(defn db-invoke [db id args]
  (apply (core/->fn db id) args))

(declare indexes->min-index-position)

;;;;;;;;;;;;;;;;;;;
;; DATABASE QSQR ;;
;;;;;;;;;;;;;;;;;;;

(defn boundedness [tuple]
  (keyword
   (str
    (if (u/lvar? (nth tuple 0 '_)) \_ \e)
    (if (u/lvar? (nth tuple 1 '_)) \_ \a)
    (if (u/lvar? (nth tuple 2 '_)) \_ \v)
    (if (u/lvar? (nth tuple 3 '_)) \_ \t))))

(defn eidify
  [^eva.Database db [e a v t added :as in]]
  [(if (and e (not (symbol? e)))
     (or (.entid db e) e) e)
   (if (and a (not (symbol? a)))
     (or (.entid db a) a) a)
   (if (and v
            (not (symbol? v))
            (attr/ref-attr? db a))
     (or (.entid db v) v) v)
   t
   added])

(defn needs-resolution?
  [x]
  (and x (not (symbol? x))))

(defn- resolving
  "Custom xform for batch-eidify. Tests indicate comparable performance to previous
  implementation; should have smaller memory print. (shrug)"
  [db]
  (fn [rf]
    (let [a (volatile! nil)
          fs (volatile! [needs-resolution? ;; e
                         (fn [attr]        ;; a
                           (vreset! a attr)
                           (needs-resolution? attr))
                         (fn [v]           ;; v
                           (and (needs-resolution? v)
                                (attr/ref-attr? db @a)))
                         (constantly false) ;; t
                         (constantly false)])] ;; added
      (fn ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [fs-now @fs]
           (vswap! fs next)
           (rf result (MapEntry/create ((first fs-now) input) input))))))))

(defn batch-eidify
  [^eva.Database db eavtas]
  (let [e+a+v+t+as (sequence (mapcat #(eduction (resolving db) %))
                             eavtas)
        n (count (first eavtas))
        eidifieds (partition-map key
                                 {true (fn [xs]
                                         (.entids db (vals xs)))
                                  false vals}
                                 e+a+v+t+as)]
    (map vec (partition-all n eidifieds))))

(defn untransform
  [transform-map datoms]
  (for [[e a v t added?] datoms]
    (dat/datom (get-in transform-map [:e e] e)
               (get-in transform-map [:a a] a)
               (get-in transform-map [:v v] v)
               t added?)))

(defn extensions-impl
  [db terms-coll]
  (let [exemplar (first terms-coll)
        bound (boundedness exemplar)
        _ (when (contains? #{:____ :__v_ :___t :__vt} bound)
            (raise :query/insufficient-binding
                   "Insufficient binding. Would cause full db scan."
                   {:binding bound
                    :exemplar exemplar}))
        eavtas->eidified (zipmap terms-coll (batch-eidify db terms-coll))
        eidified (vals eavtas->eidified)
        transform-maps (map (fn [[eavta eidified]] ;; <== ORDER IS IMPORTANT
                              (transduce (comp (zip-from eavta)
                                               (zip-from [:e :a :v :t :added?])
                                               (filter (fn [[k [x y]]] (not= x y))))
                                         (completing (fn [m [k [x y]]] (assoc-in m [k y] x)))
                                         {}
                                         eidified))
                            eavtas->eidified)
        equal-v (fn [v] (fn [d] (= (:v d) v)))
        equal-t (fn [t] (fn [d] (= (:tx d) t)))
        project (case bound ;; creating functions that project to the appropriate components of the datom.
                  (:e___ :e_v_ :e__t :e_vt) (partial map (partial take 1)) ;; TODO: better projection functions.
                  (:_a__ :_a_t) (partial map (comp (partial take 1) rest))
                  (:ea__ :ea_t) (partial map (partial take 2))
                  :_av_ (partial map (comp (partial take 2) rest))
                  :_avt (partial map (comp (partial take 2) rest))
                  :eav_ (partial map (partial take 3))
                  :eavt (partial map (partial take 4)))
        projected (project eidified)
        alternate-raw (try (case bound ;; And here we do it all in parallel: Boo-yah!
                             :e___ (core/multi-select-datoms-ordered db (cons :eavt projected))
                             :_a__ (core/multi-select-datoms-ordered db (cons :aevt projected))

                             :ea__ (core/multi-select-datoms-ordered db (cons :eavt projected))
                             :e_v_ (map (fn [[_ _ v _] results] (filter (equal-v v) results))
                                        eidified (core/multi-select-datoms-ordered db (cons :eavt projected)))
                             :e__t (map (fn [[_ _ _ t] results] (filter (equal-t t) results))
                                        eidified (core/multi-select-datoms-ordered db (cons :eavt projected)))
                             :_av_ (core/multi-select-datoms-ordered db (cons :avet projected))
                             :_a_t (map (fn [[_ _ _ t] results] (filter (equal-t t) results))
                                        eidified (core/multi-select-datoms-ordered db (cons :aevt projected)))
                             :eav_ (core/multi-select-datoms-ordered db (cons :eavt projected))
                             :ea_t (map (fn [[_ _ _ t] results] (filter (equal-t t) results))
                                        eidified (core/multi-select-datoms-ordered db (cons :eavt projected)))
                             :e_vt (map (fn [[_ _ v t] results] (filter (every-pred (equal-t t) (equal-v v))
                                                                        results))
                                        eidified (core/multi-select-datoms-ordered db (cons :eavt projected)))
                             :_avt (map (fn [[_ _ _ t] results] (filter (equal-t t) results))
                                        eidified (core/multi-select-datoms-ordered db (cons :avet projected)))
                             :eavt (core/multi-select-datoms-ordered db (cons :eavt projected))))
        project* (apply juxt (take (count exemplar) [:e :a :v :tx :added]))
        untransformed (mapcat untransform transform-maps alternate-raw) ;; <== ORDER IS IMPORTANT. ASSUMPTION IS THAT THEY CORRESPOND!!
        alternate-result (map project* untransformed)]
    alternate-result))

;;;;;;;;;;;;;;;;;;;
;; DATABASE CORE ;;
;;;;;;;;;;;;;;;;;;;

(def ^:private split-idx-name
  (memoize
   (fn split-index-name* [k] (mapv (comp keyword str) (name k)))))

(defn resolve-component [^eva.Database db index-name t->c t c]
  (case t
    (:e :a :t) (.entid db c)
    :v (if (= :vaet index-name)
         ;; :vaet is contracted to only hold entities.
         (.entid db c)
         ;; in all other indexes, a > v, so we can lookup:
         (when-let [attr (attr/resolve-attribute db (t->c :a))]
           (if (attr/ref-attr? attr)
             (.entid db c)
             c)))))

;; TODO: the following function can/should be changed into a form that exploits
;;       batch resolution.
(defn resolve-components
  [db index-name components]
  (let [comp-types (split-idx-name index-name)
        t->c       (zipmap comp-types components)]
    (into []
          (sequence (comp (map vector)
                          (map (fn [[t c]] (resolve-component db index-name t->c t c))))
                    comp-types
                    components))))

(declare as-of roots->min-index-position read-range-retry log-entry->db)

(def query-edb-timer
  (let [timer (metrics/timer "This times calls to eva.query/extensions on Database -- used by the query engine.")]
    (metrics/get-or-register metrics/DEFAULT ["eva.database" "extensions" "timer"] timer)))

(def ^:dynamic index-names #{:aevt :avet :eavt :vaet})

(defn insist-valid-index-name [index-name]
  (insist (contains? index-names index-name)
          (format "Got index-name %s expected one of: %s" index-name index-names)))

(extend-protocol core/EntidCoercionType
  Long
  (entid-coercion-type [_] :eid)
  EntityID
  (entid-coercion-type [_] :eid)
  clojure.lang.Keyword
  (entid-coercion-type [_] :keyword)
  java.util.List
  (entid-coercion-type [_] :list)
  Integer
  (entid-coercion-type [_] :integer)
  nil
  (entid-coercion-type [_] :nil))

(defn entid-type [id]
  (if-not (satisfies? core/EntidCoercionType id)
    (core/raise-entid-coercion
     :illegal-type
     (format "cannot resolve object of type %s to entity id" (type id))
     {:id id})
    (core/entid-coercion-type id)))

(defrecord Database [basis-t
                     store
                     log-entry
                     indexes
                     idents
                     attrs
                     parts
                     db-fns
                     database-info
                     lookup-ref-cache]
  UpdatableDB
  (advance-db* [this log-entry]
    (insist (= (:tx-num log-entry) (inc (core/tx-num this)))
            (format "the provided tx-log-entry is not the successor to the current db state. %s %s"
                    (:tx-num log-entry) (inc (core/tx-num this))))
    (let [tx-datoms (entry->datoms log-entry)
          indexes' (tracing "eva.database/advance-db*::advance-overlay"
                            (over/advance-overlay indexes this log-entry))
          ;; TODO: Handle schema retractions
          by-attr (group-by :a tx-datoms)
          idents' (into idents (map (juxt :v :e)) (get by-attr *db-ident-eid*))
          parts'  (dats->partitions idents' (count parts) (get by-attr *install-part-eid*))
          attrs'  (dats->attrs indexes' idents' (get by-attr *install-attr-eid*))
          db-fns' (update-db-fns db-fns idents' (get by-attr *db-fn-eid*))]
      (-> this
          (assoc :basis-t (:tx-num log-entry)
                 :log-entry log-entry
                 :indexes indexes'
                 :db-fns db-fns'
                 :idents idents'
                 :lookup-ref-cache (atom {}))
          (#(merge-with into % {:parts  parts'
                                :attrs  attrs'})))))

  (safe-advance-db [this new-entry]
    (insist (not (:speculative? new-entry)))
    (tracing "eva.database/safe-advance-db"
             (if (not= (:index-roots new-entry) (:index-roots log-entry))
                 ;; the indexes have advanced, we can flush our current state and start fresh
               (log-entry->db database-info store (open-transaction-log store database-info) new-entry)
               (advance-db* this new-entry))))

  (speculatively-advance-db [this log-entry]
    (insist (:speculative? log-entry))
    (advance-db* this log-entry))

  (advance-db-to-tx [this tx-log target-tx-num]
    (if (<= target-tx-num (core/tx-num this))
      this
      (let [tx-backlog (lazy-read-range tx-log (inc (core/tx-num this)) (inc target-tx-num))]
        (reduce safe-advance-db this tx-backlog))))

  attr/ResolveAttribute
  (resolve-attribute [db identifier]
    (get-in db [:attrs (.entid db identifier)]))

  (resolve-attribute-strict [db identifier]
    (if-let [attr (attr/resolve-attribute db identifier)]
      attr
      (attr/raise-attribute-non-resolution :unresolvable-attribute
                                      "unable to resolve."
                                      {:identifier identifier})))

  core/SelectDatoms
  (select-datoms [db [index-name & components :as q]]
    (insist-valid-index-name index-name)
    (core/select-datoms indexes (cons index-name (core/resolve-components db index-name components))))

  core/MultiSelectDatoms
  (multi-select-datoms [db [index-name & component-colls]]
    (insist (every? (every-pred sequential? not-empty) component-colls))
    (insist-valid-index-name index-name)
    (tracing "eva.Database:multi-select-datoms"
      (core/multi-select-datoms indexes
                                (cons index-name (map #(core/resolve-components db index-name %) component-colls)))))
  (multi-select-datoms-ordered [db [index-name & component-colls]]
    (insist-valid-index-name index-name)
    (insist (every? (every-pred sequential? not-empty) component-colls))
    (tracing "eva.Database:multi-select-datoms-ordered"
      (->> component-colls
           (map #(core/resolve-components db index-name %))
           (doall)
           (cons index-name)
           (core/multi-select-datoms-ordered indexes))))

  eva.Database
  (invoke [this eid-or-ident args]
    (db-invoke this eid-or-ident args))
  (attribute [db attrId]
    (ee/with-api-error-handling
      (eva.attribute/resolve-attribute db attrId)))
  (attributeStrict [db attrId]
    (ee/with-api-error-handling
      (let [attr (.attribute db attrId)]
        (if-not (instance? eva.Attribute attr)
          (attr/raise-attribute-non-resolution :irresolvable-attribute
                                          "cannot coerce provided id to an attribute."
                                          {:identifier attrId})
          attr))))
  (basisT [_]
    (ee/with-api-error-handling basis-t))
  (asOfT [_]
    (ee/with-api-error-handling
      (when-not (= basis-t (:tx-num log-entry)) (:tx-num log-entry))))
  (snapshotT [_]
    (ee/with-api-error-handling
      (:tx-num log-entry)))
  (nextT [db]
    (ee/with-api-error-handling
      (inc (.basisT db))))

  (entid [db id]
    (ee/with-api-error-handling
      (case (entid-type id)
        :keyword (get idents id)
        :list (core/resolve-lookup-ref db id)
        :integer (long id)
        :eid id
        nil)))

  (entids [db ids]
     (ee/with-api-error-handling
       (partition-map entid-type
                      {:keyword (partial map (partial get idents))
                       :list (partial core/batch-resolve-lookup-refs db)
                       :integer (partial map long)
                       :eid identity
                       :default (constantly nil)}
                      ids)))

  (entidStrict [db id]
    (ee/with-api-error-handling
      (let [resolved-id (.entid db id)]
        (if-not (entity-id/entity-id? resolved-id)
          (core/raise-entid-coercion
           :strict-coercion-failure
           "unable to coerce provided id to entity id"
           {:id id})
          resolved-id))))

  (entidsStrict [db ids]
    (ee/with-api-error-handling
      (let [resolved-ids (.entids db ids)]
        (doseq [[id resolved-id] (map vector ids resolved-ids)]
          (when-not (entity-id/entity-id? resolved-id)
            (core/raise-entid-coercion
             :strict-coercion-failure
             "unable to coerce provided id to entity id"
             {:id id})))
        resolved-ids)))

  (ident [db id]
    (ee/with-api-error-handling
      (cond
        (keyword? id) id
        (satisfies? core/EntidCoercionType id)
        (when-let [entid (.entid db id)]
          (get (smart-maps/inverse idents) entid))
        :else
        (core/raise-ident-coercion
         :illegal-type
         (format "cannot resolve object of type %s to ident" (type id))
         {:id id}))))

  (identStrict [db id]
    (ee/with-api-error-handling
      (let [ident (.ident db id)]
        (if-not (keyword? ident)
          (core/raise-ident-coercion
           :strict-coercion-failure
           "unable to coerce provided id to ident"
           {:id id})
          ident))))

  (isExtantEntity [this id]
    (boolean (first (core/select-datoms this [:eavt (.entid this id)]))))

  (history [db]
    (ee/with-api-error-handling
      (hdb/db-snapshot->history-snapshot db extensions-impl)))
  (entity [db eid]
    (ee/with-api-error-handling (eva.entity/entity db eid)))
  (datoms [db index components]
    (ee/with-api-error-handling (core/select-datoms db (concat [index] components))))
  (pull [db pattern eid]
    (ee/with-api-error-handling (pull-query/pull db (eva-reader/ensure-parsed pattern) eid)))
  (pullMany [db pattern eids]
    (ee/with-api-error-handling (pull-query/pull-many db (eva-reader/ensure-parsed pattern) eids)))
  (asOf [db t] (ee/with-api-error-handling (as-of db t)))
  (with [this tx-data]
    (ee/with-api-error-handling
      (let [tx-report (transaction-pipeline/transact this tx-data)]
        (if-let [tx-result (transaction-pipeline/tx-result tx-report)]
          (let [speculative-log-entry (-> tx-result
                                          :log-entry
                                          (assoc :speculative? true))]
            (-> (select-keys tx-result [:tx-data :tempids])
                (assoc :db-before this
                       :db-after (speculatively-advance-db this speculative-log-entry))))
          (throw (transaction-pipeline/tx-exception tx-report))))))

  core/LogEntry
  (cur-tx-eid [db] (:cur-tx-eid log-entry))
  (cur-max-id [db] (:cur-max-id log-entry))
  (index-roots [db] (:index-roots log-entry))
  (tx-num [db] (:tx-num log-entry))

  core/ComponentResolution
  (resolve-components [this index components]
    (resolve-components this index components))

  core/DatabaseInternal
  (db-fn? [db eid]
    (contains? (:db-fns db)
               (.entid db eid)))
  (->fn [db eid]
    (let [res (get (:db-fns db) (.entidStrict db eid))]
      (if res res
          (raise :eva.database-function/no-such-function
                 (format "There is no database function %s installed in this database." eid)
                 {::sanex/sanitary? false}))))
  (resolve-eid-partition [db eid]
    (if (integer? (entity-id/partition eid))
      (do
        (insist (contains? (smart-maps/inverse (:parts db)) (entity-id/partition eid)))
        eid)
      (assoc eid :partition (pc/safe-get (:parts db) (entity-id/partition eid)))))

  (allocated-entity-id? [db eid]
    (let [p (entity-id/partition eid)]
      (if (= p *tx-partition-id*)
        (<= entity-id/base-tx-eid eid (core/cur-tx-eid db))
        (<= 0 (entity-id/n eid) (core/cur-max-id db)))))

  core/LookupRefCache
  (update-lookup-ref-cache! [db resolved-id-map]
    (swap! lookup-ref-cache merge resolved-id-map))

  core/LookupRefResolution
  ;; delegating all resolution to free functions in 'lookup-refs' namespace:
  (assert-conformant-lookup-ref [db lookup-ref]
    (lookup-refs/assert-conformant-lookup-ref db lookup-ref))

  (resolve-lookup-ref [db lookup-ref]
    (lookup-refs/resolve-lookup-ref db lookup-ref))

  (resolve-lookup-ref-strict [db lookup-ref]
    (lookup-refs/resolve-lookup-ref-strict db lookup-ref))

  (batch-resolve-lookup-refs [db lookup-refs]
    (lookup-refs/batch-resolve-lookup-refs db lookup-refs))

  (batch-resolve-lookup-refs-strict [db lookup-refs]
    (lookup-refs/batch-resolve-lookup-refs-strict db lookup-refs))

  p/EDB
  (extensions [db terms]
    (metrics/with-timer query-edb-timer
      (extensions-impl db terms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATABASE CONSTRUCTION

;; NOTE: Using this lru cache made things Sllllloooowwwwwwwww. Will need to follow up.
(def empty-lookup-ref-cache {} #_(cache/lru-cache-factory {} :threshold 100000))

(d/defn ^{::d/aspects [traced]} ->db
  "Given an object of indexes that supports select-datoms on eavt and aevt,
   realize a database instance from the datoms therein."
  [database-info indexes store log-entry]
  (let [idents (->idents indexes)
        attrs  (->attrs  indexes idents)
        db-fns (->db-fns indexes idents)
        parts  (->parts  indexes idents)]
    (Database. (:tx-num log-entry)
               store
               log-entry
               indexes
               idents
               attrs
               parts
               db-fns
               database-info
               (atom empty-lookup-ref-cache))))

(defn roots->min-index-position [index-roots]
  (->> index-roots vals (map :tx-num) (apply min)))

(defn indexes->min-index-position [indexes]
  (->> indexes vals (map (comp :tx deref)) (apply min)))

(defn read-range-retry [v x y]
  (with-retries
    (defaults/read-log-range-retry-strategy)
    "retrying read range on tx log"
    @(read-range v x y)))

(defn read-entry-retry [log]
  (with-retries
    (defaults/read-log-range-retry-strategy)
    "error while reading head of transaction-log"
    @(log-entry log)))

(d/defn ^{::d/aspects [traced]} log-entry->db
  "Given a log entry, creates a database snapshot consistent with that point in
   time.  If `state?' is set, creates a staging db capable of flushing index
   updates. Will asynchronously read the span of the transaction log needed to
   reconstitute the database's state."
  [database-info store log entry]
  (insist (not (:speculative? entry)))
  (let [log (set-log-count log (inc (:tx-num entry)))
        roots (:index-roots entry)
        basis-tx (roots->min-index-position roots)
        basis-entry @(nth log basis-tx)]
    (->db database-info (get-indexes store (:database-id database-info) entry) store entry)))

(defn log->db
  "Given the transaction log, realize a database consistent with the latest
  entry in the transaction log."
  [database-info store log]
  (log-entry->db database-info store log @(log-entry log)))

(d/defn ^{::d/aspects [(logged) traced timed]} as-of
  [^eva.Database db t]
  (let [store (:store db)
        database-info (:database-info db)
        _ (assert database-info)
        log (open-transaction-log store database-info)
        tx-num (->tx-num t)
        log-entry @(log-entry (set-log-count log (inc tx-num)) tx-num)
        basis-t (.basisT db)]
    (if (some? log-entry)
      (let [as-of-db (log-entry->db database-info store log log-entry)]
        (assoc as-of-db :basis-t (max basis-t tx-num)))
      (raise :database/as-of-does-not-exist
             (format "could not find a transaction log entry for %s" tx-num)
             {:t t, ::sanex/sanitary? false}))))
