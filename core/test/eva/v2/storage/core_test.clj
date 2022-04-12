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

(ns eva.v2.storage.core-test
  (:require [clojure.test :refer :all]
            [eva.v2.storage.core :refer [->Block write-block write-blocks read-block read-blocks create-block delete-block compare-and-set-block]]
            [clojure.data.generators :as gen]
            [clojure.tools.logging :refer [spy debug]]
            [com.stuartsierra.component :refer [start stop]]
            [schema.test]
            [clojure.tools.namespace.repl :refer [refresh]])
  (:import (eva ByteString)
           (java.util.concurrent CountDownLatch)))

(use-fixtures :once schema.test/validate-schemas)

(defn rand-bytes [n] (byte-array (for [_ (range n)] (gen/byte))))

(def notifier (agent *out*))
(defn notify [& args]
  (send-off notifier (fn [out] (binding [*out* out] (apply println args)) out)))

(defn validate-core-operations
  ([store] (validate-core-operations store (str (random-uuid)) (str (random-uuid))))
  ([store namespace id]
   (let [b0 (->Block namespace id {} (ByteString/copyFromUTF8 "hello world!"))]
     (is (true? (create-block store b0)))
     (is (= b0 (read-block store :read-full namespace (:id b0))))
     (is (= (assoc b0 :val nil)
            (read-block store :read-attrs namespace (:id b0))))
     (is (false? (create-block store (assoc b0 :val (ByteString/copyFromUTF8 "shouldn't be persisted")))))
     (is (= b0 (read-block store :read-full namespace (:id b0))))
     (is (= (select-keys b0 [:namespace :id])
            (write-block store :write-full b0)
            (write-block store :write-full b0)
            (write-block store :write-full b0)))
     (is (= b0 (read-block store :read-full namespace (:id b0))))
     (is (false? (compare-and-set-block store
                                        (assoc b0 :val (ByteString/copyFromUTF8 "unexpected val"))
                                        (assoc b0 :val (ByteString/copyFromUTF8 "swapped value")))))
     (is (= b0 (read-block store :read-full namespace (:id b0))))
     (is (compare-and-set-block store b0 (assoc b0 :val (ByteString/copyFromUTF8 "swapped value"))))
     (is (false? (compare-and-set-block store b0 (assoc b0 :val (ByteString/copyFromUTF8 "swapped value")))))
     (is (= (assoc b0 :val (ByteString/copyFromUTF8 "swapped value")) (read-block store :read-full namespace (:id b0))))
     (is (= (select-keys b0 [:namespace :id])
            (delete-block store namespace (:id b0))))
     (is (nil? (read-block store :read-full namespace (:id b0)))))))

(defn validate-concurrent-create [store n]
  (let [start (CountDownLatch. n)
        ns (str (random-uuid))
        id (str (random-uuid))
        actions (doall (for [i (range n)
                             :let [b (->Block ns id {} (ByteString/copyFromUTF8 (str i)))
                                   task (future
                                          (.await start)         ;; each future thread will block until all have reached this point
                                          (let [created? (create-block store b)]
                                            {:i i :ns ns, :id id, :created? created?}))]]
                         (do (.countDown start)
                             task)))
        results (doall (map deref actions))]
    (is (= n (count results)))
    (is (= 1 (count (filter :created? results))))
    (is (= (dec n) (count (remove :created? results))))
    ))

(defn validate-concurrent-compare-and-set-block [store n]
  (let [start (CountDownLatch. (inc n))                     ;; latch = n+1 because we need to wait until we create the initial block
        ns (str (random-uuid))
        id (str (random-uuid))
        b0 (->Block ns id {} (ByteString/copyFromUTF8 "init state"))
        cas-actions (doall (for [i (range n)
                             :let [b (->Block ns id {} (ByteString/copyFromUTF8 (str i)))
                                   task (future
                                          (.await start)         ;; each future thread will block until all have reached this point
                                          (let [swapped? (compare-and-set-block store b0 b)]
                                            {:i i :ns ns, :id id, :swapped? swapped?}))]]
                         (do (.countDown start)
                             task)))]
    (is (true? (create-block store b0)))
    (.countDown start)                                      ;; created initial block, cas-actions can proceed
    (let [results (doall (map deref cas-actions))]
      #_(println results)
      (is (= n (count results)))
      (is (= 1 (count (filter :swapped? results))))
      (is (= (dec n) (count (remove :swapped? results))))
      (is (= (str (:i (first (filter :swapped? results))))
             (-> (read-block store :read-full ns id)
                 :val
                 .toByteArray
                 (String. "UTF-8"))))
      #_(println (-> (read-block store :read-full ns id)
                   :val
                   .toByteArray
                   (String. "UTF-8"))))
    ))

(defn test-storage
  ([{:as opts
     :keys [concurrency]
     :or   {concurrency 100}}
    create-store & args]
   (let [store (start (apply create-store args))]
     (try
       (validate-core-operations store)
       (validate-concurrent-create store concurrency)
       (validate-concurrent-compare-and-set-block store concurrency)
       (finally (stop store)))
     )))

(comment
  (test:local-storage)

  #_(deftest test:mariadb-storage
    ;; for use with the mariadb docker container started by `./dev/integration-testing/mariadb/start-test-database.sh`
    (test-storage {} sql/map->SQLStorage {:db-spec "jdbc:mariadb://localhost:3306/eva?user=eva&password=notasecret"}))

  (test:mariadb-storage)
  )
