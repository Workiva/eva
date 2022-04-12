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

(ns eva.v2.datastructures.var-test
  (:require [clojure.test :refer :all]
            [eva.v2.datastructures.var :refer :all]
            [eva.datastructures.protocols :refer [make-editable!]]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.value-store.core :as value]
            [eva.v2.storage.value-store :refer [put-value]]))

(defn memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (random-uuid)
   ::value/partition-id (random-uuid)})

(deftest var-unit-tests
  (let [config (memory-config)]
    (qp/testing-for-resource-leaks
     (try
       (let [store (qu/acquire vsc/concurrent-value-store-manager :testing config)
             pk (str ["1"] ["v"])
             p-var (persisted-var store pk)
             p-at (make-editable! p-var)]
         (is @(put-value @store pk 1))
         (is (= 1 @p-var))
         (is (= 1 @p-at))
         (compare-and-set! p-at 1 2)
         (is (= 2 @p-var))
         (is (= 2 @p-at)))
       (finally
         (qu/release* vsc/concurrent-value-store-manager :testing config true))))))
