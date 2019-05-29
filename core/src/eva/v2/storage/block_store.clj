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

(ns eva.v2.storage.block-store
  (:require [quartermaster.core :as qu]
            [eva.v2.storage.block-store.types :as types]
            [eva.v2.storage.block-store.impl.ddb :as ddb]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.block-store.impl.sql :as sql]))

;; CONSTRUCTORS & IDENT ;;

(defmethod types/config-by-type ::types/ddb [_] ::ddb/config)
(defmethod types/build-block-store ::types/ddb [config] (ddb/build-ddb-store config))
(defmethod types/block-store-ident ::types/ddb [config] (ddb/ddb-store-ident config))

(defmethod types/config-by-type ::types/ddb-local [_] ::ddb/local-config)
(defmethod types/build-block-store ::types/ddb-local [config] (ddb/build-ddb-local-store config))
(defmethod types/block-store-ident ::types/ddb-local [config] (ddb/ddb-local-store-ident config))

(defmethod types/config-by-type ::types/memory [_] ::memory/config)
(defmethod types/build-block-store ::types/memory [config] (memory/build-memory-store config))
(defmethod types/block-store-ident ::types/memory [config] (memory/memory-store-ident config))

(defmethod types/config-by-type ::types/sql [_] ::sql/config)
(defmethod types/build-block-store ::types/sql [config] (sql/build-sql-store config))
(defmethod types/block-store-ident ::types/sql [config] (sql/sql-store-ident config))

;; END CONSTRUCTORS & IDENT ;;

(qu/defmanager block-store-manager
  :discriminator
  (fn [_ config] (types/block-store-ident config))
  :constructor
  (fn [_ config] (types/build-block-store config)))
