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

(ns eva.v2.storage.testing-utils
  (:require [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.value-store.core :as value]
            [eva.v2.storage.block-store :as bs]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.value-store.concurrent :as vsc]))

(defn store-memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (random-uuid)
   ::value/partition-id (random-uuid)})

(defmacro with-mem-value-store [name & body]
  `(qp/testing-for-resource-leaks
    (let [~name (qu/acquire vsc/concurrent-value-store-manager :testing (store-memory-config))]
      (try
        ~@body
        (finally (qu/release ~name true))))))

(defmacro with-mem-block-store [name & body]
  `(qp/testing-for-resource-leaks
    (let [~name (qu/acquire bs/block-store-manager :testing (store-memory-config))]
      (try
        ~@body
        (finally (qu/release ~name true))))))
