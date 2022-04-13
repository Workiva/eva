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

(ns eva.v2.transaction-pipeline.resolve.ids
  "Contains the logic for three major chunks of work in the transaction pipeline:
   1. Resolve the unique-attribute-merging problem that arises from unique identity semantics.
   2. Resolve temporary ids into permanent ids.
   3. Eliminate redundant Adds or Retracts in tx-data."
  (:require [eva.attribute :as attr]
            [eva.utils :refer [one]]
            [eva.entity-id :as entity-id :refer [temp? tagged-tempid? permify-id ->Long perm?]]
            [eva.error :refer [raise]]
            [recide.sanex :as sanex]
            [loom.graph :as lg]
            [loom.alg :as la]
            [eva.core :as core]
            [utiliva.core :refer [partition-pmap keepcat group-by]]
            [eva.v2.transaction-pipeline.type.basic :refer [->final-retract add? retract?]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]])
  (:import [java.util List]
           [eva.v2.transaction_pipeline.type.basic Add Retract])
  (:refer-clojure :exclude [group-by]))

(defn validate-unique-value-semantics
  "Attempts to assert a new tempid with a unique value already in the database will cause an Exception instead of merging."
  [name-cc]
  (loop [[[a v :as av] & ns] name-cc
         unique-value-attr-name nil
         tempid-name nil
         permid-name nil]
    (when (and unique-value-attr-name tempid-name permid-name)
      (raise :transact-exception/unique-value-violation
             "Cannot assert a new temp-id with the same :db.unique/value attribute-value pair as an extant eid"
             {:attribute (attr/ident (first unique-value-attr-name))
              :tempid (second tempid-name)
              :permid (second permid-name)
              ::sanex/sanitary? false}))
    (when-not (nil? av)
      (let [uvan (or unique-value-attr-name
                     (when (and (not= :db/id a)
                                (= :db.unique/value (attr/unique a)))
                       av))
            tempid-name (or tempid-name (when (and (= :db/id a) (temp? v)) av))
            permid-name (or permid-name (when (and (= :db/id a) (perm? v)) av))]
        (recur ns uvan tempid-name permid-name)))))

(defn build-required-equivalences
  ([db name-ccs]
   (reduce (partial build-required-equivalences db) {} name-ccs))
  ([db res name-cc]
   (validate-unique-value-semantics name-cc)
   (let [ids (sequence (comp (filter #(= :db/id (first %))) ;; attribute
                             (map second)) ;; value
                       name-cc)
         exemplar (reduce entity-id/select-exemplar-id ids)
         perm (when (temp? exemplar) (permify-id exemplar))
         final-id (->> (or perm exemplar)
                       (core/resolve-eid-partition db)
                       (entity-id/->Long))
         aliases (into {}
                       (keep (fn [n] (when (not= n final-id) [n final-id])))
                       ids)]
     (merge res aliases))))

(d/defn ^{::d/aspects [traced]} derive-aliases
  "Given the TxData, derive the entity ids that must
   be equivalent to yield a valid database state.
   Returns a map:
   {:equivalences [#{ent-ids-that-must-be-merged}]
    :aliases      {ent-id --> exemplar-id-under-equivalences}}"
  [db tx-data]
  (let [unique-attr-ops (->> tx-data
                             (group-by (filter (fn [cmd] (attr/unique (:attr cmd))))
                                       :attr))
        unique-name-edges (for [[a cmds] unique-attr-ops
                                {:keys [e v]} cmds
                                :let [uav   [a v]
                                      db-id [:db/id e]]]
                            [db-id uav])
        uavs (map second unique-name-edges)
        perm-unique-datoms (->> uavs
                                (map #(update % 0 attr/id))
                                (cons :avet)
                                (core/multi-select-datoms-ordered db))
        extant-unique-edges (for [[uav pds] (zipmap uavs perm-unique-datoms)
                                  :when (not-empty pds)
                                  pd pds]
                              [[:db/id (:e pd)] uav])
        name-graph (reduce (fn [grph uname-edges]
                             (lg/add-edges grph uname-edges))
                           (lg/graph)
                           (concat unique-name-edges extant-unique-edges))
        required-equivs (->> (la/connected-components name-graph)
                             (build-required-equivalences db))]
    required-equivs))

(defn transform-ids [resolve-aliases cmd]
  (cond-> (update cmd :e resolve-aliases)
    (attr/ref-attr? (:attr cmd)) (update :v resolve-aliases)))

(defn ->aliases-function [db aliases-atom]
  (fn [eid]
    (let [aliases' (swap!
                    aliases-atom
                    (fn [aliases eid]
                      (cond-> aliases
                        (temp? eid)
                        (update eid #(if (nil? %) (->Long (core/resolve-eid-partition db (permify-id eid))) %))))
                    eid)]
      (get aliases' eid eid))))

(defn resolve-lookup-refs
  "Finds all entities in (:tx-data report) which are lookup refs (i.e. pairs [uniq-attr-id value])
   and resolves them to entity id.
   Returns modified report with all lookup refs resolved:
   {:tx-data [...
             {:e [uniq-attr-1 value-1] :a a1 :v v1}
             {:e e2 :a a2 :v [uniq-attr-2 value-2]}
             ....]}
   replaced with resolved entity ids:
   {:tx-data [...
             {:e e1 :a a1 :v v1}
             {:e e2 :a a2 :v v2}
             ....]}
   Note: Replacements happen either at entities position or values position, and do not happen in attribute position.
         The are couple reasons for this: Datomic doc is not consistent about lookup refs at attribute position - they
         are allowed for list commands and are not allowed for maps. Also making attribute dependent on some value
         is kind of 'dispatching' attribute by value which is (advanced?) technique with not very clear purpose."
  [db tx-report]
  (let [lookup-ref?       (fn [kind]
                            (fn [eid] (instance? List (some-> eid kind))))
        e-lookup-ref?     (lookup-ref? :e)
        v-lookup-ref?     (lookup-ref? :v)
        ->v-lookup-ref    (fn [eid]
                            (when (and (v-lookup-ref? eid) (attr/ref-attr? (:attr eid)))
                              [eid (:v eid)]))
        ->e-lookup-ref    (fn [eid]
                            (when (e-lookup-ref? eid)
                              [eid (:e eid)]))
        tx-data           (:tx-data tx-report)
        e-lookup-ref-map  (into {} (keep ->e-lookup-ref) tx-data)
        v-lookup-ref-map  (into {} (keep ->v-lookup-ref) tx-data)
        resolved-entities (zipmap (keys e-lookup-ref-map)
                                  (core/batch-resolve-lookup-refs-strict db (vals e-lookup-ref-map)))
        resolved-values   (zipmap (keys v-lookup-ref-map)
                                  (core/batch-resolve-lookup-refs-strict db (vals v-lookup-ref-map)))
        tx-data           (map (fn [eid]
                                 (let [resolved-entity (get resolved-entities eid)
                                       resolved-value  (get resolved-values eid)]
                                   (cond-> eid
                                     (some? resolved-entity) (assoc :e resolved-entity)
                                     (some? resolved-value)  (assoc :v resolved-value)
                                     :true                   (identity))))
                               tx-data)]
    (assoc tx-report :tx-data tx-data)))

(d/defn ^{::d/aspects [traced]} resolve-ids
  [report]
  (let [db-before (:db-before report)
        _ (entity-id/set-max-allocated-id! (core/cur-max-id db-before) (core/cur-tx-eid db-before))
        report  (resolve-lookup-refs db-before report)
        aliases (derive-aliases db-before (:tx-data report))
        tempids (atom aliases)
        resolve-aliases (->aliases-function db-before tempids)]
    (-> report
        (update :tx-data (partial into #{} (map (partial transform-ids resolve-aliases))))
        (assoc :tempids
               (into {} (map (fn [[k v]] [(->Long (core/resolve-eid-partition db-before k)) v]))
                     @tempids)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn prune-cardinality-many
  "Given the subset of tx-data where every Add or Retract is on a card many
   attribute, returns a new collection of tx-data where commands redundant with
   the current state of the database have been replaced with nil.

   Commands that are *not* spurious are wrapped in a vector to match the
   contract for prune-cardinality-one."
  [db tx-data]
  (let [qs (->> tx-data
                (map (juxt :e :a :v))
                (cons :eavt)
                (core/multi-select-datoms-ordered db))]
    (map (fn [cmd qs]
           (let [q (one qs)]
             (condp instance? cmd
               Add     (when (nil? q) [cmd])
               Retract (when-not (nil? q) [cmd]))))
         tx-data
         qs)))

(defn prune-cardinality-one
  "Given the subset of tx-data where every Add or Retract is on a card one
   attribute, returns a new collection of tx-data where commands redundant with
   the current state of the database have been replaced with nil and makes the
   set of implicit retractions on extant entity+attributes explicit.

   Returns a collection of nils and vectors of commands:
   [nil [[:db/add ...]] [[:db/add e a v1] [:db/retract e a v0]]]"
  [db tx-data]
  (let [qs (->> tx-data
                (map (juxt :e :a))
                (cons :eavt)
                (core/multi-select-datoms-ordered db))]
    (map (fn [cmd qs]
           (let [q (one qs)]
             (condp instance? cmd
               Add     (if (nil? q)
                         [cmd]
                         (when (not= (:v q) (:v cmd))
                           [cmd (->final-retract db (:e q) (:a q) (:v q) (:attr cmd))]))
               Retract (when (= (:v q) (:v cmd)) [cmd]))))
         tx-data
         qs)))

(defn batch-prune-superfluous
  [db tx-data]
  (sequence
   (partition-pmap (comp attr/cardinality :attr)
                   {:db.cardinality/one   (partial prune-cardinality-one db)
                    :db.cardinality/many  (partial prune-cardinality-many db)}
                   tx-data)))

(d/defn ^{::d/aspects [traced]} eliminate-redundancy
  "Eliminate all tx-data that are redundant with the current state of
   the database. Will also add any implicit retractions required on
   cardinality one attributes."
  [report]
  (update report :tx-data #(into #{} (keepcat) (batch-prune-superfluous (:db-before report) %))))
