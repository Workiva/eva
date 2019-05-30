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

(ns eva.entity-id-test
  (:require [clojure.test :refer :all]
            [eva.entity-id :as entity-id]))

(defn bin->long [xs]
  (let [parse-char #(case % 0 0, \0 0, 1 1, \1 1)
        xs (mapv parse-char xs)
        len (count xs)
        negated? (and (= 64 len)
                      (= 1 (first xs)))
        xs (if (= 64 len) (rest xs) xs)
        n (Long/parseLong (reduce str "" xs) 2)]
    (if negated?
      (bit-set n 63)
      n)))

(deftest test:pack-entity-id
  (are [packed-id part ent ret? tmp?]
       (= packed-id (entity-id/pack-entity-id part ent ret? tmp?))

    0
    0 0 false false

    1
    0 1 false false

    -9223372036854775808
    0 0 false true

    4611686018427387904
    0 0 true false

    -4611686018427387904
    0 0 true true

    (bit-shift-left 1 42)
    1 0 false false

    (bin->long "1100000000000000000000111111111111111111111111111111111111111111")
    0 entity-id/max-e true true

    (bin->long "1111111111111111111111111111111111111111111111111111111111111111")
    entity-id/max-p entity-id/max-e true true

    (bin->long "0011111111111111111111111111111111111111111111111111111111111111")
    entity-id/max-p entity-id/max-e false false))

(deftest test:peid->added
  (are [a? packed-id] (= a? (entity-id/added? packed-id))
    false (bin->long "1100000000000000000000111111111111111111111111111111111111111111")
    true  (bin->long "1000000000000000000000111111111111111111111111111111111111111111")
    false (bin->long "0100000000000000000000111111111111111111111111111111111111111111")
    true  (bin->long "0011111111111111111111111111111111111111111111111111111111111111"))

  (doseq [p (range 10)
          e (range 10)
          :let [a? (rand-nth [true false])
                temp? (rand-nth [true false])]]
    (is (= a? (entity-id/added? (entity-id/pack-entity-id p e (not a?) temp?))))))
