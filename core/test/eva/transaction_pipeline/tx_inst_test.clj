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

(ns eva.transaction-pipeline.lookup-refs-test
  (:require [eva.api :as eva]
            [eva.v2.transaction-pipeline.validation :as val]
            [eva.error :as err]
            [eva.config :refer [with-overrides]]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [clojure.test :refer :all]
            [bond.james :as bond])
  (:import (java.util.concurrent ExecutionException ConcurrentLinkedQueue)
           (java.util Date)))

(defn ->custom-now-fn [times]
  (let [q (ConcurrentLinkedQueue. times)]
    (fn [] (Date. ^Long (.poll q)))))

(defn skew-test
  [skew-window skew-times expected-results?]
  (with-local-mem-connection conn
    (with-overrides {:eva.transaction-pipeline.clock-skew-window skew-window}
      (with-redefs [val/current-time (->custom-now-fn skew-times)]
        (let [results
              (doall
               (for [_ skew-times]
                 (try @(eva/transact conn [[:db/add (eva/tempid :db.part/user) :db/doc "foo"]])
                      (catch Exception e e))))]
          (is (expected-results? results)))))))

(deftest unit:skew-tolerance
  (testing "nil skew tolerance"
    (skew-test nil [1 2 3] (partial every? map?))
    (skew-test nil [1 2 1 2] (fn [[r0 r1 r2 r3]]
                               (and (map? r0)
                                    (map? r1)
                                    (instance? Exception r2)
                                    (map? r3)))))

  (testing "zero skew tolerance"
    (skew-test 0 [1 2 3] (partial every? map?))
    (skew-test 0 [1 2 1 2] (fn [[r0 r1 r2 r3]]
                             (and (map? r0)
                                  (map? r1)
                                  (instance? Exception r2)
                                  (map? r3)))))

  (testing "light skew tolerance"
    (skew-test 100 [10 20 10 20 30] (partial every? map?))
    (skew-test 100 [1000 2000 1000 2000] (fn [[r0 r1 r2 r3]]
                                           (and (map? r0)
                                                (map? r1)
                                                (instance? Exception r2)
                                                (map? r3)))))

  (testing "heavy skew tolerance"
    (skew-test 1e10 (repeatedly 100 #(rand-int 1e5)) (partial every? map?))))
