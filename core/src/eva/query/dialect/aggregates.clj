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

(ns eva.query.dialect.aggregates
  "Aggregates usable in the :find specification of queries."
  (:require [utiliva.comparator :as cmp]
            [eva.query.error :refer [raise-builtin]]
            [recide.sanex :as sanex])
  (:refer-clojure :exclude [eval min max rand]))

(defn min
  ([xs] (when-not (empty? xs) (apply (partial cmp/min compare) xs)))
  ([n xs] (when-not (empty? xs) (take n (sort xs)))))

(defn max
  ([xs] (when-not (empty? xs) (apply (partial cmp/max compare) xs)))
  ([n xs] (when-not (empty? xs) (take n (sort (comp - compare) xs)))))

(defn rand [n c] (repeatedly n (partial rand-nth (vec c))))
(defn sample [n c] (->> c distinct shuffle (take n)))
(def count-distinct (comp count distinct))
(defn sum [c] (when-not (empty? c)
                (try (reduce + c)
                     (catch Throwable t
                       (raise-builtin :aggregate "sum failed (possibly operating on non-numbers?)"
                                      {:sample-elems (take 4 c),
                                       ::sanex/sanitary? false}
                                      t)))))
(defn avg [c] (when-not (empty? c)
                (double (/ (sum c) (count c)))))

(defn median ;; naive
  [c]
  (let [c (sort c)]
    (if (odd? (count c))
      (nth c (Math/floor (/ (count c) 2.0)))
      (avg (take 2 (drop (dec (/ (count c) 2.0)) c))))))
