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

(ns eva.v2.database.overlay
  (:require [eva.concurrent.background-resource-map :as brm]
            [eva.core :as core
             :refer [entry->datoms safe-advance-index-better batch-advance-index]]
            [eva.v2.database.index :refer [db->type-attr-eids
                                    open-indexes]]
            [eva.utils.delay-queue :as d-q]
            [barometer.aspects :refer [timed]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]
            [plumbing.core :as pc]))

(def background-processed-idxs #{:eavt :aevt :avet :vaet})
(def resource-priorities (zipmap [:eavt :aevt :avet :vaet] (range)))

(defn resource-selector
  [tag->delay]
  (apply min-key
         (comp resource-priorities key)
         tag->delay))

(defprotocol Overlay
  (advance-overlay [indexes db log-entry])
  (batch-advance-overlay [indexes tx-log tx-log-entry]))

(defn indexes->min-index-position [indexes]
  (->> indexes (vals) (map (comp :tx deref)) (apply min)))

(d/defn ^{::d/aspects [traced timed]} wait-on-index-updates [index] @index)

(defrecord OverlaidIndexes [indexes bg-resource-map basis-tx]
  core/SelectDatoms
  (select-datoms [this [index-name & _ :as q]]
    (core/select-datoms (wait-on-index-updates (get indexes index-name)) q))
  core/MultiSelectDatoms
  (multi-select-datoms [this [index-name & _ :as q]]
    (core/multi-select-datoms (wait-on-index-updates (get indexes index-name)) q))
  (multi-select-datoms-ordered [this [index-name & _ :as q]]
    (core/multi-select-datoms-ordered (wait-on-index-updates (get indexes index-name)) q))
  Overlay
  (advance-overlay [this db log-entry]
    (let [ref-attr-eids (db->type-attr-eids db :db.type/ref)
          byte-attr-eids (db->type-attr-eids db :db.type/bytes)]
      (assoc this
             :indexes
             (pc/for-map [[name idx] (:indexes this)]
                         name
                         (if (background-processed-idxs name)
                           (brm/enqueue-background-update bg-resource-map
                                                          idx
                                                          name
                                                          safe-advance-index-better
                                                          ref-attr-eids
                                                          byte-attr-eids
                                                          log-entry)
                           (d-q/enqueue-update idx
                                               safe-advance-index-better
                                               ref-attr-eids
                                               byte-attr-eids
                                               log-entry))))))
  (batch-advance-overlay [this tx-log tx-log-entry]
    (assoc this :indexes
           (pc/for-map [[name idx] (:indexes this)]
                       name
                       (if (background-processed-idxs name)
                         (brm/enqueue-background-update bg-resource-map
                                                        idx
                                                        name
                                                        batch-advance-index
                                                        tx-log
                                                        tx-log-entry)
                         (d-q/enqueue-update idx
                                             batch-advance-index
                                             tx-log
                                             tx-log-entry))))))

(defn empty-overlay [store roots]
  (let [indexes (open-indexes store roots)]
    (->OverlaidIndexes indexes
                       (brm/background-resource-map resource-selector)
                       (indexes->min-index-position indexes))))
