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

(ns eva.v2.storage.ddb-test
  (:require [clojure.test :refer :all]
            [eva.v2.storage.ddb :refer :all]
            [eva.v2.storage.core :as blocks]
            [clojure.java.io :as io]
            [clojure.set :as set]))
(def ^:dynamic *ddb-client* nil)
(defn ddb-client [] (assert *ddb-client* "*ddb-client* not bound") *ddb-client*)

(defn random-unprocessed [ks]
  (let [completed (random-sample 0.75 ks)]
    {:completed   completed
     :unprocessed (set/difference (set ks) (set completed))}))

(deftest test:retry-until-complete
  (let [items (vec (range 1000))
        call-count (atom 0)
        strategy (exponential-retry-strategy 100)
        final-results (retry-until-complete strategy
                                            :completed
                                            :unprocessed
                                            (fn ([] []) ([acc complete] (into acc complete)))
                                            #(do (swap! call-count inc) (random-unprocessed %))
                                            items)]
    (is (< 0 @call-count (count strategy)))
    (is (= (set final-results)
           (set items)))))

