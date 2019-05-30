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

(ns eva.byte-string-test
  (:require [clojure.test :refer :all])
  (:import (eva ByteString)
           (java.util Arrays)))

(deftest test:byte-string-joining
  (let [b1-bytes (repeat (rand-int 100) (byte 65))
        b1 (byte-array b1-bytes)
        b2-bytes (repeat (rand-int 100) (byte 66))
        b2 (byte-array b2-bytes)
        b3-bytes (repeat (rand-int 100) (byte 67))
        b3 (byte-array b3-bytes)
        expected-array (byte-array (concat b1-bytes b2-bytes b3-bytes))]
    (is (= (ByteString/copyFrom expected-array)
           (ByteString/join (map #(ByteString/copyFrom ^bytes %) [b1 b2 b3]))))
    (is (Arrays/equals expected-array
                       (->> [b1 b2 b3]
                            (map #(ByteString/copyFrom ^bytes %))
                            (ByteString/join)
                            (.toByteArray))))))
