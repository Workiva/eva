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

(ns eva.datom
  (:refer-clojure :exclude [partition])
  (:require [eva.entity-id :as entity-id]
            [eva.datastructures.utils.comparators :refer [UPPER LOWER]]
            [eva.datastructures.utils.interval :refer [open-interval]]
            [clojure.pprint :as pp]
            [schema.core :as s]
            [com.rpl.specter :as sp]
            [com.rpl.specter.macros :refer [transform]]
            [eva.sizing-api :as sapi]
            [eva.print-ext]
            [plumbing.core :as pc])
  (:import (java.io Writer)
           (com.carrotsearch.sizeof RamUsageEstimator)
           (eva Datom)))

(defprotocol PackableDatom
  (->peid  [d])
  (->eavt  [d])
  (->eavth [d])
  (->avet  [d])
  (->aveth [d])
  (->aevt  [d])
  (->aevth [d])
  (->vaet  [d])
  (->vaeth [d])
  (->log   [d]))

(defn pack [d index-name]
  (case index-name
    :eavt  (->eavt  d)
    :eavth (->eavth d)
    :avet  (->avet  d)
    :aveth (->aveth d)
    :aevt  (->aevt  d)
    :aevth (->aevth d)
    :vaet  (->vaet  d)
    :vaeth (->vaeth d)
    :log   (->log   d)))

;; hash caching pattern from clojure.core
(defmacro ^:private caching-int-fn [coll f k]
  `(let [h# ~k]
     (if-not (== h# (int -1))
       h#
       (let [h# (~f ~coll)]
         (set! ~k (int h#))
         h#))))

(defn ->interval [c0 c1 c2]
  (open-interval [c0 c1 c2 LOWER] [c0 c1 c2 UPPER]))

(defn hash-unordered [collection]
  (-> (reduce unchecked-add-int 0 (map hash collection))
      (mix-collection-hash (count collection))))

(deftype DatomMap [e a v tx added
                   ^:unsynchronized-mutable ^int _hash
                   ^:unsynchronized-mutable ^int _hasheq
                   ^:unsynchronized-mutable ^int _size
                   _meta]
  java.io.Serializable
  sapi/SizeEstimable
  (ram-size [this]
    (caching-int-fn this #(RamUsageEstimator/sizeOf %) _size))
  Datom
  (e [_] e)
  (a [_] a)
  (v [_] v)
  (tx [_] tx)
  (added [_] added)
  (getIndex [this i] (nth this i))
  (getKey [this k]
    (case k
      (:e "e") e
      (:a "a") a
      (:v "v") v
      (:tx "tx") tx
      (:added "added") added))
  clojure.lang.IObj
  (withMeta [this m]
    (DatomMap. e a v tx added _hash _hasheq _size m))
  (meta [this] _meta)
  clojure.lang.IHashEq
  (hasheq [this] (hash-unordered this))
  Object
  (hashCode [this] (bit-xor (.hashCode (class this))
                            (.hashCode e)
                            (.hashCode a)
                            (.hashCode v)
                            (.hashCode tx)
                            (.hashCode added)))
  (equals [this other]
    (or (identical? this other)
        (and (instance? Datom other)
             (= e (.e ^Datom other))
             (= a (.a ^Datom other))
             (= v (.v ^Datom other))
             (= tx (.tx ^Datom other))
             (= added (.added ^Datom other)))))
  (toString [this] (pr-str this))
  clojure.lang.IPersistentMap
  (count [_] 5)
  (equiv [this that]
    (and (instance? Datom that)
         (= e (.e ^Datom that))
         (= a (.a ^Datom that))
         (= v (.v ^Datom that))
         (= tx (.tx ^Datom that))
         (= added (.added ^Datom that))))
  (seq [this] (seq [e a v tx added]))
  (assoc [this k v']
    (case k
      (:e "e") (DatomMap. v' a v tx added -1 -1 -1 nil)
      (:a "a") (DatomMap. e v' v tx added -1 -1 -1 nil)
      (:v "v") (DatomMap. e a v' tx added -1 -1 -1 nil)
      (:tx "tx") (DatomMap. e a v v' added -1 -1 -1 nil)
      (:added "added") (DatomMap. e a v tx v' -1 -1 -1 nil)))
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k not-found]
    (case k
      (:e "e") e
      (:a "a") a
      (:v "v") v
      (:tx "tx") tx
      (:added "added") added
      :else not-found))
  clojure.lang.Indexed
  (nth [_ i]
    (case i
      0 e
      1 a
      2 v
      3 tx
      4 added))
  (nth [this i not-found]
    (if (<= 0 i 4)
      (nth this i)
      not-found))
  PackableDatom
  (->peid [d] (if added e (bit-set e 62)))
  (->eavt  [d]
    (if added
      [:conj [e a v tx]]
      [:remove-interval (->interval e a v)]))
  (->avet  [d]
    (if added
      [:conj [a v e tx]]
      [:remove-interval (->interval a v e)]))
  (->aevt  [d]
    (if added
      [:conj [a e v tx]]
      [:remove-interval (->interval a e v)]))
  (->vaet  [d]
    (if added
      [:conj [v a e tx]]
      [:remove-interval (->interval v a e)]))
  (->aevth [d] [:conj [a (->peid d) v tx]])
  (->aveth [d] [:conj [a v (->peid d) tx]])
  (->eavth [d] [:conj [(->peid d) a v tx]])
  (->vaeth [d] [:conj [v a (->peid d) tx]])
  (->log [d] [(->peid d) a v]))

(defn datom
  ([[e a v tx added]] (datom e a v tx added))
  ([tx [op e a v]]    (datom e a v tx (pc/safe-get {:db/add true, :db/retract false} op)))
  ([e a v tx added]  (DatomMap. e a v tx added -1 -1 -1 nil)))

(defn vec->datom
  ([index [c0 c1 c2 c3 & [added]]]
   ;; The canonical ordering is e a v t added
   ;; the incoming vector may be ordered differently
   ;; if so, we need to swap some stuff around
   (cond
     (= index :eavt)
     (datom c0 c1 c2 c3 (or added true))

     (= index :aevt)
     (datom c1 c0 c2 c3 (or added true))

     (= index :avet)
     (datom c2 c0 c1 c3 (or added true))

     (= index :vaet)
     (datom c2 c1 c0 c3 (or added true))

     :else
     (throw (RuntimeException. (str "Index: " index " is non-canonical")))))
  ([[e a v tx & [added]]]
   (datom e a v tx (or added true))))

(defn ->logical-lower [d] (assoc d :tx LOWER))

(defn ->logical-upper [d] (assoc d :tx UPPER))

(defn valid-temp-id? [x] (< x 0))

(defn temp-tx-id? [x]
  (and (entity-id/entity-id? x)
       (entity-id/temp? x)
       (or (= 1 (entity-id/partition x))
           (= :db.part/tx (entity-id/partition x)))))

(defn tagged-temp-id? [x]
  (and (entity-id/entity-id? x)
       (entity-id/temp? x)
       (entity-id/n x)))

(defn unpack-datom
  ([tx [peid a v]]
   (unpack-datom [peid a v tx]))
  ([[peid a v tx :as d]]
   (let [added (entity-id/added? peid)]
     (datom (bit-clear peid 62) a v tx added))))

(defn unpack
  ([index-name [c0 c1 c2 c3]]
   (case index-name
     :eavt (datom c0 c1 c2 c3 true)
     :aevt (datom c1 c0 c2 c3 true)
     :avet (datom c2 c0 c1 c3 true)
     :vaet (datom c2 c1 c0 c3 true)
     :eavth (datom (bit-clear c0 62) c1 c2 c3 (not (bit-test c0 62)))
     :aevth (datom (bit-clear c1 62) c0 c2 c3 (not (bit-test c1 62)))
     :aveth (datom (bit-clear c2 62) c0 c1 c3 (not (bit-test c2 62)))
     :vaeth (datom (bit-clear c2 62) c1 c0 c3 (not (bit-test c2 62))))))

(defn add? [^Datom d] (.added d))
(defn retract? [^Datom d] (not (.added d)))

(defn ->adds [coll] (filter add? coll))

(defn ->retracts [coll] (filter retract? coll))

(def datom-write-handler
  {DatomMap {"eva/datom"
             (reify org.fressian.handlers.WriteHandler
               (write [_ w d]
                 (.writeTag w "eva/datom" 5)
                 (.writeObject w (.e ^Datom d))
                 (.writeObject w (.a ^Datom d))
                 (.writeObject w (.v ^Datom d))
                 (.writeObject w (.tx ^Datom d))
                 (.writeObject w (.added ^Datom d))))}})

(def datom-read-handler
  {"eva/datom"
   (reify org.fressian.handlers.ReadHandler
     (read [_ r tag component-count]
       (DatomMap. (.readObject r)
                  (.readObject r)
                  (.readObject r)
                  (.readObject r)
                  (.readObject r) -1 -1 -1 nil)))})
