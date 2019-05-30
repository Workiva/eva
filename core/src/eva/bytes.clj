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

;; -----------------------------------------------------------------------------
;; This file uses implementation from Clojure which is distributed under the
;; Eclipse Public License (EPLv1.0) at https://github.com/clojure/clojure with the
;; following notice:
;;
;; Copyright (c) Rich Hickey. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html in the 'third-party' folder of
;; this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;; -----------------------------------------------------------------------------

(ns eva.bytes
  (:require [eva.error :refer [insist]])
  (:import [com.google.common.primitives UnsignedBytes]
           [org.fressian.handlers WriteHandler ReadHandler]))

(def byte-cmptr (UnsignedBytes/lexicographicalComparator))

;; hash caching pattern from clojure.core
(defmacro ^:private caching-hash [coll hash-fn hash-key]
  `(let [h# ~hash-key]
     (if-not (== h# (int -1))
       h#
       (let [h# (~hash-fn ~coll)]
         (set! ~hash-key (int h#))
         h#))))

;; better byte array
(deftype BBA [^bytes ba ^:unsynchronized-mutable ^int _hash]
  java.lang.Comparable
  (compareTo [this that]
    (.compare ^java.util.Comparator byte-cmptr
              (.-ba this)
              (.-ba ^BBA that)))
  Object
  (equals [this that]
    (if (= (type this) (type that))
      (zero? (.compareTo this that))
      false))
  (hashCode [this]
    (caching-hash this (fn juahash [^BBA bba] (bit-xor (java.util.Arrays/hashCode ^bytes (.ba bba)) 31)) _hash)))

(defn byte-array? [o]
  (let [c (class o)]
    (and (.isArray c)
         (identical? (.getComponentType c) Byte/TYPE))))

(defn bbyte-array [o]
  (insist (byte-array? o))
  (->BBA o -1))

(defn bbyte-array? [o]
  (instance? BBA o))

(defn ensure-bytes-wrapped [o]
  (if (bbyte-array? o)
    o (bbyte-array o)))

(def bba-write-handler
  {BBA {"eva/bba"
        (reify WriteHandler
          (write [_ w bba]
            (.writeTag w "eva/bba" 1)
            (.writeObject w (.ba ^BBA bba))))}})

(def bba-read-handler
  {"eva/bba"
   (reify ReadHandler
     (read [_ r tag component-count]
       (let [ba (.readObject r)]
         (->BBA ba -1))))})
