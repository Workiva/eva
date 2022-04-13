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

(ns eva.value-types
  (:require [eva.entity-id :refer [IEntityID]]
            [eva.bytes]
            [eva.functions])
  (:import [eva.functions DBFn]
           [eva.bytes BBA]
           (java.util Date UUID)
           (java.net URI)
           (clojure.lang BigInt)))

(defn test-array
  [t]
  (let [check (type (t []))]
    (fn [arg] (instance? check arg))))

(def byte-array?
  (test-array byte-array))

(def type->validator
  {:db.type/instant (partial instance? Date)
   :db.type/boolean (partial instance? Boolean)
   :db.type/bytes   (partial instance? BBA)
   :db.type/uri     (partial instance? URI)
   :db.type/uuid    (partial instance? UUID)
   :db.type/string  string?
   :db.type/keyword keyword?
   :db.type/ref     (partial satisfies? IEntityID)
   :db.type/bigdec  decimal?
   :db.type/float   (fn [x] (and (float? x) (not (Float/isNaN x))))
   :db.type/bigint  (partial instance? BigInt)
   :db.type/double  (fn [x] (and (double? x) (not (Double/isNaN x))))
   :db.type/long    (partial instance? Long)
   :db.type/fn      (partial instance? DBFn)})

(defmulti valid-value-type? (fn [t _] t))

(defn register-types [vmap]
  (doseq [[t vfn] vmap]
    (defmethod valid-value-type? t [_ v] (vfn v))))

(register-types type->validator)
