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

(ns eva.comparators
  (:require [eva.datastructures.utils.comparators
             :refer [defcomparator
                     bounded-seq-comparator
                     bound
                     bounded-proj-comparator]]
            [eva.error :refer [insist]])
  (:import (eva Datom)))

(defn ->full-proj-sym [s] (symbol (str s "-full-proj-cmp")))
(defn ->index-sym [s] (symbol (str s "-index-cmp")))

(defmacro defcomparators
  "Explodes out to define a set of comparators"
  [cmp-name & projs-and-cmps]
  (insist (even? (count projs-and-cmps)))
  (let [cmps (take-nth 2 (rest projs-and-cmps))]
    `(do (defcomparator ~(->index-sym cmp-name)
           (bounded-seq-comparator ~@cmps))
         (defcomparator ~(->full-proj-sym cmp-name)
           (bounded-proj-comparator ~@projs-and-cmps)))))

(defn lng-cmp [x y]
  (Long/compare x y))

(defn -e [^Datom d] (.e d))
(defn -a [^Datom d] (.a d))
(defn -v [^Datom d] (.v d))
(defn -tx [^Datom d] (.tx d))
(defcomparators eavt
  -e lng-cmp
  -a lng-cmp
  -v compare
  -tx lng-cmp)

(defcomparators aevt
  -a lng-cmp
  -e lng-cmp
  -v compare
  -tx lng-cmp)

(defcomparators avet
  -a lng-cmp
  -v compare
  -e lng-cmp
  -tx lng-cmp)

(defcomparators vaet
  -v lng-cmp
  -a lng-cmp
  -e lng-cmp
  -tx lng-cmp)

(defn index-cmp [index-name]
  (cond
    (contains? #{"eavt" "eavth" :eavt :eavth} index-name) eavt-index-cmp
    (contains? #{"aevt" "aevth" :aevt :aevth} index-name) aevt-index-cmp
    (contains? #{"avet" "aveth" :avet :aveth} index-name) avet-index-cmp
    (contains? #{"vaet" "vaeth" :vaet :vaeth} index-name) vaet-index-cmp))

(defn full-proj-cmp [index-name]
  (cond
    (contains? #{"eavt" "eavth" :eavt :eavth} index-name) eavt-full-proj-cmp
    (contains? #{"aevt" "aevth" :aevt :aevth} index-name) aevt-full-proj-cmp
    (contains? #{"avet" "aveth" :avet :aveth} index-name) avet-full-proj-cmp
    (contains? #{"vaet" "vaeth" :vaet :vaeth} index-name) vaet-full-proj-cmp))
