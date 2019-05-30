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

(ns eva.v2.database.history-snapshot
  (:require [eva.core :as core]
            [eva.error :refer [insist]]
            [eva.query.datalog.protocols :as p]
            [barometer.core :as metrics]
            [eva.error :as ee :refer [raise]]
            [utiliva.core :refer [zip-to zip-from partition-map group-like]])
  (:import (java.io Writer)))

;; The historic database is a variant of the database that includes *all* datoms
;; across time and supports:
;;
;; Implemented:
;;  - datoms
;;  - queries
;;
;; Not yet implemented TODO:
;;  - index-range (not sure if this matters)
;;  - as-of
;;  - since
;;
;; queries will retrieve *all* datoms, which can be distinguished by the added
;; field.

(defn do-on-index [f x]
  (fn [i v] (if-not (= x i) v (f v))))

(def e-pos {:eavt 0 :aevt 1 :avet 2 :vaet 2})
(def name->hname {:eavt :eavth :aevt :aevth :avet :aveth :vaet :vaeth})

(defn- build-historic-components
  "To select datoms for a given entity-id, we need to be able to handle both
   the asserted and retracted forms of the entity id. For a given set of
   components to select from an index, this function expands the components to
   both the added and retracted entity ids, *or* casts to only include the
   added xor retracted datoms."
  [index-name components]
  (let [added? (nth components 5 ::unbound)]
    (case added?
      ;; expand to include adds and retracts (deduped, provided we don't have an e)
      ::unbound (distinct [components
                           (map-indexed (do-on-index #(bit-flip % 62)
                                                     (e-pos index-name))
                                        components)])
      ;; only select adds
      true [(map-indexed (do-on-index #(bit-clear % 62) (e-pos index-name)) components)]
      ;; only select retracts
      false [(map-indexed (do-on-index #(bit-set % 62) (e-pos index-name)) components)])))

(defn build-historic-criteria
  [index-name components]
  (cons (name->hname index-name) (build-historic-components index-name components)))

;; for now, we assume all reads from the historic database are multi-select-ordered
(defrecord HistoryOverlay [indexes basis-tx]
  core/MultiSelectDatoms
  (multi-select-datoms-ordered [this [index-name & _ :as q]]
    (insist (contains? #{:eavth :aevth :aveth :vaeth} index-name))
    (core/multi-select-datoms-ordered @(get indexes index-name) q)))

(def historic-query-edb-timer
  (let [timer (metrics/timer "This times calls to eva.query/extensions on a Historic Database -- used by the query engine.")]
    (metrics/get-or-register metrics/DEFAULT ["eva.historic-database" "extensions" "timer"] timer)))

(defn db-snapshot->history-snapshot [^eva.Database db extensions-impl]
  (let [history-indexes (map->HistoryOverlay (:indexes db))]
   (reify
      eva.Database
      ;; pass through
      (entid [_ id] (.entid db id))
      (entidStrict [_ id] (.entidStrict db id))
      (entids [_ ids] (.entids db ids))
      (entidsStrict [_ ids] (.entidsStrict db ids))
      (ident [_ entid] (.ident db entid))
      (identStrict [_ entid] (.identStrict db entid))
      (attribute [_ attr-id] (.attribute db attr-id))
      (attributeStrict [_ attr-id] (.attributeStrict db attr-id))
      (asOf [_ t] (db-snapshot->history-snapshot (.asOf db t)))

      core/ComponentResolution
      (resolve-components [_ index components] (core/resolve-components db index components))
      (batch-resolve-components [_ index components] (core/batch-resolve-components db index components))

      ;; history specific
      core/SelectDatoms
      (datoms [db index components] (core/select-datoms db (concat [index] components)))

      (select-datoms [db [index-name & components]]
        (reduce into
                (core/multi-select-datoms-ordered history-indexes
                                                  (->> components
                                                       (core/resolve-components db index-name)
                                                       (build-historic-criteria index-name)))))
      core/MultiSelectDatoms
      (multi-select-datoms-ordered [db [index-name & component-colls]]
        (insist (every? (every-pred sequential? not-empty) component-colls))
        ;; to support the fully-general multi-select, we need to be able to expand
        ;; some of the components out to their historic equivalent components,
        ;; select from the database, and then coerce back to the original
        ;; shape of the query.
        (let [new-inputs (sequence (comp (map (partial core/resolve-components db index-name))
                                         (map (partial build-historic-components index-name)))
                                   component-colls)
              res (->> (reduce into new-inputs)
                       (cons (name->hname index-name))
                       (core/multi-select-datoms-ordered history-indexes))
              res (map (partial into () cat) (group-like res new-inputs))]
          res))

      p/EDB
      (extensions [db terms]
        ;; given the above implementations of select-datoms and multi-select-datoms-ordered,
        ;; we're able to directly reuse the extensional query function, since the engine
        ;; itself is agnostic to whether the database is historic or not.
        ;;
        ;; NOTE: we're not reusing the non-history db's impl s.t. we can have an
        ;;       independent metric for history queries.
        (metrics/with-timer historic-query-edb-timer
          (let [res (extensions-impl db terms)]
            res))))))
