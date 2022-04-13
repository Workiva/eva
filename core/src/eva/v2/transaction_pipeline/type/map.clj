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

(ns eva.v2.transaction-pipeline.type.map
  "Defines a concrete type for Map Entity operations handled in the transaction pipeline."
  (:require [eva.attribute :refer [rm_* reverse-attr? rm_ cardinality]]
            [eva.error :refer [raise]]
            [recide.sanex :as sanex]
            [eva.v2.transaction-pipeline.type.basic :refer [->add]]
            [clojure.string :as string])
  (:import [java.util List]
           [eva Database]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Map Entities

(defrecord MapEntity [db ^java.util.Map raw normalized])

(defn keywordize [k]
  (cond (string? k)
        (if (string/starts-with? k ":")
          (keyword (subs k 1))
          (keyword k))

        (keyword? k) k

        :else (raise :transact-exception/expected-string-or-keyword
                     (format "Expected string or keyword for map key. Received: %s of type: %s" k (type k))
                     {:key k
                      ::sanex/sanitary? false})))

(defn normalize-kv [db [k v]]
  (let [k' (keywordize k)]
    (if (= :db/id k')
      [k' (.entid ^Database db v)]
      [k' v])))

(defn normalize-map-entity [db me]
  (into {} (map (partial normalize-kv db)) me))

(defn map-entity? [o] (instance? MapEntity o))

(defn ->map-entity [db m]
  (->MapEntity db m (normalize-map-entity db m)))

(defn map->adds
  "Given a flattened MapEntity object, returns a list of Adds corresponding
   to the expanded entity."
  [^MapEntity me]
  (let [db ^Database (:db me)
        nm (:normalized me)
        eid (:db/id nm)
        -dbid (dissoc nm :db/id)]
    (when-not eid
      (raise :transact-exception/missing-db-id
             "Map entity forms must contain a :db/id"
             {:tx-data nm
              ::sanex/sanitary? false}))
    (when (empty? -dbid)
      (raise :transact-exception/missing-attributes
             "Map entity forms must contain attributes other than :db/id"
             {:tx-data nm
              ::sanex/sanitary? false}))
    (into [] cat
          (for [[a v] -dbid
                :let [rev? (reverse-attr? a)
                      lst? (instance? List v)
                      card (cardinality db (rm_* a))
                      card-many? (= :db.cardinality/many card)]]
            (cond
              ;; reverse pointing to a list of 'parents'
              (and rev? lst?)
              (for [v' v] (->add db [:db/add v' (rm_ a) eid]))

              ;; reverse pointing to a single 'parent'
              rev?
              [(->add db [:db/add v (rm_ a) eid])]

              ;; card-many + list as value ==> need to expand / flatten to multiple adds
              (and card-many? lst?)
              (for [v' v] (->add db [:db/add eid a v']))

              :else ;; pointer to a single value (card one or many)
              [(->add db [:db/add eid a v])])))))
