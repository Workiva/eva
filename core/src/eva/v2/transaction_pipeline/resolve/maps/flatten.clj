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

(ns eva.v2.transaction-pipeline.resolve.maps.flatten
  "Provides the logic for recursively flattening map entities in a transaction."
  (:require [eva.v2.transaction-pipeline.type.map :refer [->map-entity normalize-map-entity map-entity?]]
            [eva
             [attribute :refer [ref-attr? ref-attr?* is-component? unique
                                cardinality rm_* card-many? resolve-attribute-strict]]
             [entity-id :refer [entity-id? tempid partition entid-strict]]
             [error :refer [raise]]]
            [recide.sanex :as sanex])
  (:refer-clojure :exclude [partition])
  (:import [java.util Collection Map]
           [eva.v2.transaction_pipeline.type.map MapEntity]))

(def keyset (comp set keys))

(defn nested-id [^MapEntity parent ^Map child]
  (if (contains? child :db/id)
    (:db/id child)
    (let [parent-id (:db/id parent)]
      (tempid (partition parent-id)))))

(defn construct-child
  [db m child attr id]
  (when-not (or (contains? child :db/id)
                (is-component? db attr)
                (some (partial unique db)
                      (disj (set (keys child)) :db/id)))
    (raise :transact-exception/dangling-nested-map-entity
           "Nested entity maps must be under a component attribute, contain a unique attribute, or have an explicit :db/id"
           {:tx-data m
            ::sanex/sanitary? false}))
  (->map-entity db (assoc child :db/id id)))

(defn non-record-map? [m]
  (and (not (record? m))
       (instance? Map m)))

;; Forward declaration for recursive use in `recursively-flatten`
(declare flatten-map-entity)
(defn- recursively-flatten
  "Used by flatten-map-entity."
  [^MapEntity m]
  (let [db (:db m)
        nm (:normalized m)]
    ;; iterating over the normalized av pairs, looking for places to expand.
    (loop [[[a v :as av] & avs] (seq nm)
           m' {}
           res []]
      ;; TODO: VV abstraction needed.
      (let [cmany-ref? (and (some? a)
                            (not= :db/id a)
                            (card-many? db (rm_* a)))]
        (cond (nil? av)
              (conj res (->map-entity db m'))

              (non-record-map? v)
              (let [nmap (normalize-map-entity db v)
                    id (nested-id nm nmap)
                    child (construct-child db nm nmap a id)]
                (recur avs (assoc m' a id) (concat res (flatten-map-entity child))))

              ;; nested collection of maps under a card-many
              (and cmany-ref? (instance? Collection v))
              (let [grpd (group-by non-record-map? v)
                    nmaps (map (partial normalize-map-entity db) (get grpd true)) ;; TODO: transduce
                    ids (map (partial nested-id nm) nmaps)
                    children (for [[id child] (zipmap ids nmaps)]
                               (construct-child db nm child a id))
                    flattened-children (mapcat flatten-map-entity children)
                    new-v (into (get grpd false) ids)]
                (recur avs
                       (if (not-empty new-v)
                         (assoc m' a new-v)
                         m')
                       (into res flattened-children)))

              :else
              (recur avs (assoc m' a v) res))))))


(defn flatten-map-entity
  "Given an entity map, return a sequence of one or more map entities flattened
   with db/ids and references inserted where required"
  [^MapEntity me]
  (let [m (:normalized me)]
    (cond (= (keyset m) #{:db/id})
          (raise :transact-exception/missing-attributes
                 "Map entity forms must contain attributes other than :db/id"
                 {:tx-data m
                  ::sanex/sanitary? false})

          (not (contains? m :db/id))
          (raise :transact-exception/missing-db-id
                 "Map entity forms must contain a :db/id"
                 {:tx-data m
                  ::sanex/sanitary? false})

          :else
          (recursively-flatten me))))

(defn flatten-map-entities
  "Given a report comprised of Add, Retract, and MapEntity objects (possibly
   with nested Maps), return an updated report where any nested Map objects have
   been flattened into the tx-data and normalized to MapEntity objects
   containing only keyword-type attributes."
  [report]
  (let [grpd (group-by map-entity? (:tx-data report))]
    (assoc report :tx-data (into (get grpd false)
                                 (mapcat flatten-map-entity (get grpd true))))))
