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

(ns eva.utils.delay-queue-test
  (:require [eva.utils.delay-queue :refer :all]
            [clojure.test :refer :all]))

(deftest delay-queue:very-deep
  (is (= 500000500000
         @(loop [n 1000000 res (delay-queue 0)]
            (if (zero? n)
              res
              (recur (dec n) (enqueue-f res #(+ % n) [])))))))

(deftest delay-queue:memoizes
  (let [test-size 10000
        total (atom 0)
        update-fn (fn [x]
                    (enqueue-f x #(do (swap! total inc)
                                      (inc %))
                               []))
        dawdles (take test-size (rest (iterate update-fn (delay-queue 0))))
        final (last dawdles)
        dawdles (doseq [dawdle (shuffle dawdles)]
                  (deref dawdle))]
    (is (= @total test-size))
    (is (= @final test-size))))
