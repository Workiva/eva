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

(ns eva.datastructures.utils.core
  (:require [eva.error :refer [insist]]))

(defn overlaps?
  "Given a comparator and two tuples representing ranges, returns true if the ranges overlap."
  [^java.util.Comparator cmp [x1 x2] [y1 y2]]
  (and (<= (.compare cmp x1 y2) 0)
       (<= (.compare cmp y1 x2) 0)))

(defn arg-cmp
  "arg-foo in the spirit of argmin or argmax."
  ([cmp f coll]
   (arg-cmp cmp f (first coll) (f (first coll)) (next coll)))
  ([cmp f x fx coll]
   (if coll
     (let [fy (f (first coll))]
       (if (= -1 (. ^java.util.Comparator cmp (compare fy fx)))
         (recur cmp f (first coll) fy (next coll))
         (recur cmp f x fx (next coll))))
     x)))

(defn fast-last [coll]
  (let [my-count (count coll)]
    (when-not (zero? my-count)
      (nth coll (dec my-count)))))
