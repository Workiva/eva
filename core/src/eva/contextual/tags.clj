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

(ns eva.contextual.tags
  (:require [clojure.spec.alpha :as s]
            [eva.contextual.config :as contextual-config]
            [eva.config :as config]
            [eva.v2.database.core :as database]))

(s/def ::database-id string?)

(defn config->tags [config]
  {::database-id (::database/id config)})

(defn conn->tags [conn]
  (config->tags (:config conn)))

(defn db->tags [db]
  {::database-id (:database-id (:database-info db))})

(defn log->tags [log]
  (config->tags (-> log :pv .store deref :config)))


;; syncing contextual config
(contextual-config/set-tag! ::database-id (config/config-strict :eva.telemetry.enable-per-db-metrics))
