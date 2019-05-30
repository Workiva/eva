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

(ns eva.alpha
  (:require [eva.entity-id :as e]
            [utiliva.core :as uc]
            [map-experiments.smart-maps.protocol :as smart-maps]))

(defn to-ex-data
  "(alpha)

   Given a j.u.c.ExecutionException thrown in transact as the result of
   a thrown IExceptionInfo inside a transaction function, extract the ex-data included
   within the user-defined IExceptionInfo."
  [juce]
  (-> juce .getCause .getData :exception eva.Util/read :data))

(defn to-ex-cause
  "(alpha)

   Given a j.u.c.ExecutionException thrown in transact as the result of
   a thrown IExceptionInfo inside a transaction function, extract the cause message
   included within the user-defined IExceptionInfo."
  [juce]
  (-> juce .getCause .getData :exception eva.Util/read :cause))

(defn integer->tempid [db tempid]
  (let [n (e/n tempid)
        part-id->part-key (-> db :parts smart-maps/inverse)
        part (-> (e/partition tempid) part-id->part-key)]
    (e/tempid part (- n))))

(defn tx-result->EntityID-tempids [{:as tx-result :keys [tempids db-after]}]
  (into {} (uc/map-keys (partial integer->tempid db-after)) tempids))
