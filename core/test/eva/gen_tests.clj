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

(ns eva.gen-tests
  (:require [eva.gen-test-scaffold :refer :all]
            [eva.api :as eva :refer [connect release squuid transact db]]
            [clojure.test :refer :all]
            [eva.datastructures.utils.comparators :refer [UPPER LOWER]]
            [com.rpl.specter.macros :refer [transform]]
            [clojure.tools.logging :refer [debug]]
            [eva.logging :refer [with-context]]
            [eva.v2.server.transactor-test-utils
             :refer [with-local-mem-connection with-local-sql-connection]]))

(def tx-times (atom []))
(defn ->ms [x]
  (float (/ x 1e6)))

(defn local-db-path
  [filename]
  (str (System/getProperty "user.dir") "/" filename))

(def ns-num (atom 0))

(defn next-ns
  []
  (str  "n" (swap! ns-num (comp #(mod % 1) inc))))

(defn sample-seq
  "Return a sequence of realized values from `generator`."
  ([generator seed]
   (sample-seq generator 100))
  ([generator seed max-size]
   (let [r (clojure.test.check.random/make-random)
         size-seq (clojure.test.check.generators/make-size-range-seq max-size)]
     (clojure.core/map (comp clojure.test.check.rose-tree/root #(clojure.test.check.generators/call-gen generator %1 %2))
                       (clojure.test.check.generators/lazy-random-states r)
                       size-seq))))

(defn tx-looper
  [conn]
  (let [conns [conn]
        seed (System/currentTimeMillis)]
    (reset! tx-times [])
    (let [threads
          (doall
           (for [[i conn] (map-indexed vector conns)]
             (future
               (dotimes [n 1001]
                 (when (= 0 (mod n 100)) (debug "-- thread" i "tx" n "--"))
                 (let [db (eva/db conn)]
                   (let [tx-payload (into [] (first (take 1 (sample-seq (->tx-gen db) seed 1))))]
                     (try (let [start (. java.lang.System (clojure.core/nanoTime))
                                res @(transact conn tx-payload)
                                duration (- (. java.lang.System (clojure.core/nanoTime)) start)]
                            (swap! tx-times conj duration)
                            res)
                          (catch Exception e
                            (let [ed (ex-data (.getCause e))]
                              (if-let [err-type (:eva/error ed)]
                                (if (contains? #{:transact-exception/invalid-attribute-ident
                                                 :transact-exception/merge-conflict
                                                 :transact-exception/unique-value-violation
                                                 :transact-exception/no-corresponding-attribute-installation
                                                 :transact-exception/cannot-modify-schema
                                                 :transact-exception/incomplete-install-attribute
                                                 :transact-exception/incomplete-install-partition
                                                 :transact-exception/incompatible-attribute-properties
                                                 :transact-exception/cardinality-one-violation
                                                 :transact-exception/reserved-namespace}
                                               (:eva/error ed))
                                  (do (debug "-- thread" i "tx" n "--")
                                      (debug (.getMessage e)))
                                  (do (debug "-- thread" i "tx" n "--")
                                      (debug "found exception of unknown type:" err-type)
                                      (debug "test-check seed:" seed)
                                      (debug (.getMessage e))
                                      (throw e)))
                                (do (debug "-- thread" i "tx" n "--")
                                    (debug "exception did not follow normal exception propagation rules")
                                    (debug "test-check seed:" seed)
                                    (throw e)))))))))
               conn)))]
      (mapv deref threads)
      (dorun (map #(release %) conns)))))


;; TODO: write better generative transaction tests. These aren't great.


(deftest sim:tx-loop
  (testing "with local in-memory connection"
    (with-local-mem-connection conn1
      (tx-looper conn1)))
  (testing "with local h2 connection"
    (with-local-sql-connection conn2
      (tx-looper conn2))))
