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

(ns eva.v2.transaction-pipeline.resolve.maps
  (:require [eva.v2.transaction-pipeline.resolve.maps.flatten :refer [flatten-map-entities]]
            [eva.v2.transaction-pipeline.type.map :refer [map-entity? map->adds]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]))

(d/defn ^{::d/aspects [traced]} resolve-map-entities
  "Given a report comprised of Add, Retract, and MapEntity objects, return an
   updated report where the MapEntity objects have been resolved to Adds."
  [report]
  (let [flattened-report (flatten-map-entities report)
        grpd (group-by map-entity? (:tx-data flattened-report))
        res (mapcat map->adds (get grpd true))]
    (assoc flattened-report
           :tx-data (into (get grpd false) res))))
