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

(ns eva.datastructures.versioning-test
  (:require [eva.v2.datastructures.bbtree :as bbtree]
            [eva.v2.datastructures.bbtree.api :as v0-api]
            [eva.v2.datastructures.bbtree.storage :refer [uuid]]
            [eva.v2.datastructures.bbtree.fressian :as v0-fressian]
            [eva.datastructures.test-version.api :as new-api]
            [eva.datastructures.test-version.fressian :as test-fressian]
            [eva.datastructures.protocols :as dsp]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.bytes :refer [bba-write-handler bba-read-handler]]
            [eva.datom :refer [datom-write-handler datom-read-handler]]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.value-store.core :as value]
            [flowgraph.protocols :refer [shutdown]]
            [eva.config :refer [config-strict]]
            [eva.v2.storage.value-store.handlers :refer [merge-write-handlers merge-read-handlers]]
            [clojure.test :refer :all])
  (:import [java.util UUID]))

(def write-handlers (merge-write-handlers
                     (merge bba-write-handler
                            v0-fressian/all-writers
                            datom-write-handler
                            test-fressian/all-writers)))
(def read-handlers (merge-read-handlers
                    (merge bba-read-handler
                           v0-fressian/all-readers
                           datom-read-handler
                           test-fressian/all-readers)))

(defn memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (UUID/randomUUID)
   ::value/partition-id (UUID/randomUUID)})

(deftest btree-versioning-test
  (qp/testing-for-resource-leaks
   (qu/overriding [vsc/reader-graph-manager {:discriminator (constantly :versioning-test)
                                             :constructor (fn [_ _] (vsc/reader-graph read-handlers))}
                   vsc/writer-graph-manager {:discriminator (constantly :versioning-test)
                                             :constructor (fn [_ _]
                                                            (vsc/writer-graph (config-strict :eva.v2.storage.block-size) write-handlers))}]
      (qu/acquiring [config (memory-config)
                     store (qu/acquire vsc/concurrent-value-store-manager :testing config)
                     test-data (partition 100 (shuffle (range 1E5)))
                     empty-v0-btree (v0-api/backed-sorted-set store)
                     filled-v0-btree (-> (reduce into empty-v0-btree test-data)
                                         (bbtree/persist!))]
                    (try
                      (is (thrown-with-msg? IllegalArgumentException
                                            #"Conversion from .* is undefined."
                                            (let [test-data (partition 100 (shuffle (range 1E5 2E5)))
                                                  upgraded-tree (new-api/open-writable @store (dsp/storage-id filled-v0-btree))
                                                  filled-upgraded-tree (-> (reduce into upgraded-tree test-data)
                                                                           (bbtree/persist!))]
                                              true)))
                      (is (let [_ (require 'eva.datastructures.test-version.logic.missing-versioning)
                                test-data (partition 100 (shuffle (range 1E5 2E5)))
                                upgraded-tree (new-api/open-writable @store (dsp/storage-id filled-v0-btree))
                                filled-upgraded-tree (-> (reduce into upgraded-tree test-data)
                                                         (bbtree/persist!))]
                            true))
                      (finally (qu/release* vsc/concurrent-value-store-manager :testing config true)))))))
