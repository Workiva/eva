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

(ns eva.v2.storage.value-store-test
  (:require [eva.v2.storage.value-store.concurrent :as vs]
            [eva.v2.storage.value-store :refer [put-values get-value get-values put-value create-key]]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.value-store.core :as value]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.config :as config]
            [utiliva.core :refer [zip-to]]
            [eva.v2.fressian :refer [eva-only-read-handlers eva-only-write-handlers]]
            [clojure.test :refer :all]))

(defn memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (random-uuid)
   ::value/partition-id (random-uuid)})

(defn- gen-value-str [size]
  (->> #(rand-nth "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
       (repeatedly size)
       (apply str)))

(deftest storage:request-cardinality
  (testing "max-request-cardinality: concurrent-store"
    (qp/testing-for-resource-leaks
     (with-redefs [memory/memory-store (memoize (fn [_] (memory/mem-storage 1)))]
       (let [value-store (config/with-overrides {:eva.v2.storage.max-request-cardinality 1}
                           (qu/acquire vs/concurrent-value-store-manager :testing (memory-config)))
             values (repeatedly 2 #(gen-value-str 64))
             ks (map str (range (count values)))]
         (try
           (is (= {"0" true, "1" true} @(put-values @value-store (zipmap ks values))))
           (is (= (zipmap ks values) @(get-values @value-store ["0" "1"])))
           (finally (qu/release value-store true))))))

    (qp/testing-for-resource-leaks
     (with-redefs [memory/memory-store (memoize (fn [_] (memory/mem-storage 1)))]
       (let [value-store (config/with-overrides {:eva.v2.storage.max-request-cardinality 2}
                           (qu/acquire vs/concurrent-value-store-manager :testing (memory-config)))
             values (repeatedly 2 #(gen-value-str 64))
             ks (map str (range (count values)))]
         (try
           (is (thrown? Exception @(put-values @value-store (zipmap ks values)))
               "Expected a max-request-cardinality exception but didn't receive one.")
           (is (thrown? Exception @(get-values @value-store ["0" "1"]))
               "Expected a max-request-cardinality exception but didn't receive one.")
           (finally (qu/release value-store true))))))))

(deftest storage:shard-test
  (testing "simple-shard-test"
    (dotimes [_ 40]
      (qp/testing-for-resource-leaks
       (config/with-overrides {:eva.v2.storage.block-size 512}
         (let [value-store (qu/acquire vs/concurrent-value-store-manager :testing (memory-config))
               value (gen-value-str 10000)]
           (try @(put-value @value-store "0" value)
                (is (= value @(get-value @value-store "0")))
                @(create-key @value-store "1" value)
                (is (= value @(get-value @value-store "1")))
                (finally (qu/release value-store true)))))))))
