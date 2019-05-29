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

(ns eva.datastructures.utils.comparators
  (:require [clojure.pprint :as pp]
            [utiliva.comparator :refer :all]
            [eva.datastructures.utils.fressian :as eva-fresh]
            [eva.error :refer [insist]])
  (:refer-clojure :exclude [< > <= >= min max]))
(def ^:const UPPER ::upper)
(def ^:const LOWER ::lower)

;; TODO: Attempting to memoize bounded-comparator causes an issue above in the eva code when
;;       trying to construct our comparators.  Should figure it out, since regeneration of
;;       identical comparators can be leaky.

(defn bounded-comparator
  "Bounds a comparator above and below with UPPER and LOWER."
  ^java.util.Comparator
  [cmp]
  (fn [o1 o2]
    (cond (identical? o1 o2) 0
          (identical? o2 LOWER) 1
          (identical? o1 UPPER) 1
          (identical? o2 UPPER) -1
          (identical? o1 LOWER) -1
          :else (cmp o1 o2))))

(defn bounded-comparator-above
  "Bounds a comparator above with UPPER."
  ^java.util.Comparator
  [cmp]
  (fn [o1 o2]
    (cond (identical? o1 o2) 0
          (identical? o1 UPPER) 1
          (identical? o2 UPPER) -1
          :else (cmp o1 o2))))

(defn bounded-comparator-below
  "Bounds a comparator below with LOWER."
  ^java.util.Comparator
  [cmp]
  (fn [o1 o2]
    (cond (identical? o1 o2) 0
          (identical? o2 LOWER) 1
          (identical? o1 LOWER) -1
          :else (cmp o1 o2))))

(defprotocol IBoundedComparator
  (bounded-comparator? [this])
  (bounded-above? [this])
  (bounded-below? [this]))

(extend-protocol IBoundedComparator
  Object
  (bounded-comparator? [this] false)
  (bounded-above? [this] false)
  (bounded-below? [this] false))

(defrecord Comparator [cmp raw bounded-above bounded-below serial]
  clojure.lang.IFn
  (invoke [_] (cmp))
  (invoke [_ arg0] (cmp arg0))
  (invoke [_ arg0 arg1] (cmp arg0 arg1))
  (invoke [_ arg0 arg1 arg2] (cmp arg0 arg1 arg2))
  (invoke [_ arg0 arg1 arg2 arg3] (cmp arg0 arg1 arg2 arg3))
  (invoke [_ arg0 arg1 arg2 arg3 arg4] (cmp arg0 arg1 arg2 arg3 arg4))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5] (cmp arg0 arg1 arg2 arg3 arg4 arg5))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6] (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7] (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8] (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9] (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18))
  (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19]
    (cmp arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19))
  (applyTo [_ args__8411__auto__]
    (.applyTo cmp args__8411__auto__))
  java.util.Comparator
  (compare [_ one two] (cmp one two))
  IBoundedComparator
  (bounded-comparator? [_] (or bounded-above bounded-below))
  (bounded-above? [_] (boolean bounded-above))
  (bounded-below? [_] (boolean bounded-above))
  eva-fresh/Autological
  (get-var [_] serial))

(defmacro defcomparator
  [name fun]
  (let [{bounded-above :bound-above,
         bounded-below :bound-below} (meta name)
         cmp (cond (and bounded-above bounded-below)
                   `(bound ~fun)
                   bounded-above
                   `(bound-above ~fun)
                   bounded-below
                   `(bound-below ~fun)
                   :else
                   `(let [fun# ~fun]
                      (if (instance? eva.datastructures.utils.comparators.Comparator fun#)
                        fun#
                        (->Comparator fun# fun# false false nil))))]
    `(do
       (declare ~name)
       (def ~name (assoc ~cmp :serial (var ~name))))))

(defn- pprint-comparator
  [^Comparator cmp]
  (pp/pprint-logical-block :prefix "#Comparator" :suffix ""
                           (pp/write-out (dissoc cmp :serial))))

(defmethod pp/simple-dispatch Comparator [c] (pprint-comparator c))

(defn remove-bounds
  [cmp]
  (insist (instance? Comparator cmp))
  (:raw cmp))

;; BOUNDING

(defn bound-above
  ([f]
   (if (instance? Comparator f)
     (let [raw (:raw f)
           bounding-fn (if (bounded-below? f) bounded-comparator bounded-comparator-above)]
       (->Comparator (bounding-fn raw) raw true (bounded-below? f) nil))
     (->Comparator (bounded-comparator-above f) f true false nil))))

(defn bound-below
  ([f]
   (if (instance? Comparator f)
     (let [raw (:raw f)
           bounding-fn (if (bounded-above? f) bounded-comparator bounded-comparator-below)]
       (->Comparator (bounding-fn raw) raw (bounded-above? f) true nil))
     (->Comparator (bounded-comparator-below f) f false true nil))))

(defn bound
  ([f]
   (if (instance? Comparator f)
     (if (and (bounded-above? f) (bounded-below? f))
       f
       (let [raw (:raw f)]
         (->Comparator (bounded-comparator raw) raw true true nil)))
     (->Comparator (bounded-comparator f) f true true nil))))

;; COMPOSITION

;; TODO: in interests of correctness, composition should be smart about BoundedComparator

(defmacro bounded-seq-comparator
  "Wraps the 'inner' comparators passed to create a seq-comparator using bounded-comparator.
   Additionally, the outer comparator is wrapped as well.
   eg: ((bounded-seq-comparator compare compare) [1 2] [1 UPPER]) => -1
       ((bounded-seq-comparator compare compare) UPPER [1 UPPER]) => 1"
  [& fs]
  `(bound
    (seq-comparator
     ~@(map #(list `bound %) fs))))

(defmacro bounded-proj-comparator
  "analogy:
  seq-comparator : bounded-seq-comparator :: proj-comparator : bounded-proj-comparator"
  [& fs]
  (let [projs (flatten (partition 1 2 fs))
        cmps  (flatten (partition 1 2 (rest fs)))
        args (interleave projs (map #(list bounded-comparator %) cmps))]
    `(bounded-comparator
      (proj-comparator
       ~@args))))
