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

(ns eva.v2.messaging.node.manager.alpha
  (:require [eva.v2.messaging.node.manager.types :as types]
            [eva.v2.messaging.node.beta :as beta]
            [eva.v2.messaging.node.local :as local]
            [quartermaster.core :as qu]
            [eva.v2.storage.value-store.core :as vs]
            [eva.v2.database.core :as db]
            [clojure.spec.alpha :as s]))

(defmethod types/messenger-node-discriminator :broker-uri [user-id config]
  (beta/extract-broker-config config))
(defmethod types/messenger-node-constructor :broker-uri [description config]
  (beta/create-messenger description))

(defmethod types/messenger-node-discriminator :local-messenger-node [user-id config]
  (assert (::db/id config))
  (assert (::vs/partition-id config))
  [(::db/id config) (::vs/partition-id config)])

(defmethod types/messenger-node-constructor :local-messenger-node [description config]
  (local/local-messenger))

;; END CONSTRUCTORS & IDENT ;;

(qu/defmanager messenger-nodes
  :discriminator
  (fn [user-id config] (types/messenger-node-discriminator user-id config))
  :constructor
  (fn [description config] (types/messenger-node-constructor description config)))
