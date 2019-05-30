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

(ns eva.v2.storage.block-store.impl.ddb
  (:require [eva.v2.storage.ddb :as ddb]
            [eva.v2.storage.block-store.types :as types]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::aws-region string?)
(s/def ::dynamo-table string?)
(s/def ::aws-access-key-id string?)
(s/def ::aws-secret-key string?)

(s/def ::config
  (s/keys :req [::aws-region
                ::dynamo-table]
          :opt [::aws-access-key-id ;; TODO: make spec require both auth keys or neither
                ::aws-secret-key]))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defn build-ddb-store
  [{:keys [::aws-region
           ::dynamo-table
           ::aws-access-key-id
           ::aws-secret-key]}]
  (ddb/map->DDBStorage {:table  dynamo-table
                        :config (cond-> {:region aws-region}
                                  (and aws-access-key-id aws-secret-key)
                                  (assoc :cred {:access-key-id aws-access-key-id
                                                :secret-key aws-secret-key}))}))

(defn ddb-store-ident
  [{:keys [::aws-region
           ::dynamo-table]}]
  [::types/ddb aws-region dynamo-table])

;;;;;;;;;;;;;;;
;; DDB LOCAL ;;
;;;;;;;;;;;;;;;

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::local-endpoint string?)
(s/def ::local-config
  (s/keys :req [::local-endpoint
                ::aws-region
                ::dynamo-table]
          :opt [::aws-access-key-id
                ::aws-secret-key]))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defn build-ddb-local-store
  [{:keys [::local-endpoint
           ::dynamo-table
           ::aws-access-key-id
           ::aws-secret-key]}]
  (ddb/map->DDBStorage {:table  dynamo-table
                        :config (cond-> {:endpoint local-endpoint} ;; TODO: please check
                                  (and aws-access-key-id aws-secret-key)
                                  (assoc :cred {:access-key-id aws-access-key-id
                                                :secret-key aws-secret-key}))
                        :create-table? true}))

(defn ddb-local-store-ident
  [{:keys [::local-endpoint
           ::dynamo-table]}]
  [::types/ddb-local local-endpoint dynamo-table])
