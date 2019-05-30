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

(ns eva.v2.transaction-pipeline.type.basic
  "Defines concrete types for the fundamental Add and Retract transaction operations."
  (:require [eva
             [attribute :as attr]
             [bytes :refer [ensure-bytes-wrapped]]
             [core :refer [select-datoms]]
             [entity-id :refer [entid-strict temp? entity-id?]]
             [error :refer [raise insist]]
             [utils :refer [one]]]
            [recide.sanex :as sanex]
            [eva.core :refer [db-fn? ->fn allocated-entity-id?]])
  (:import (java.util List Map)
           (eva Database)
           (eva.bytes BBA)
           (eva.attribute Attribute)
           (org.fressian.handlers WriteHandler ReadHandler)))

(defn ensure-extant-eid [db e]
  (if (allocated-entity-id? db e)
    e
    (raise :transact-exception/unallocated-entity
           (format "The entity id %s has not been allocated in this database." e)
           {:eid e})))

(defn ->valid-entity-id
  "We say the entity id `e` is valid if:
   1) `e` is a tempid
   2) `e` is resolvable as a db/ident or lookup reference
   3) `e` is in the allocated entity id range

   If it is valid, will return either a tempid or the permanent long id"
  [db e]
  (cond
    (integer? e)   (ensure-extant-eid db e)
    (entity-id? e) e
    (instance? List e) e
    :else (entid-strict db e)))

(defn cast-value [db vt v]
  (case vt
    :db.type/ref   (->valid-entity-id db v)
    :db.type/bytes (ensure-bytes-wrapped v)
    :db.type/long  (long v)
    v))

(defn validate-command-syntax!
  "Throws an exception if validation fails."
  [[op e a v :as cmd]]
  (when (not= (count cmd) 4)
    (raise :transact-exception/arity
           (format "%s command contains %s arg(s), expected 3" op (dec (count cmd)))
           {:command cmd}))
  (when (nil? e)
    (raise :transact-exception/nil-e
           (format "%s contains nil entity id" op)
           {:command cmd}))
  (when (nil? a)
    (raise :transact-exception/nil-a
           (format "%s contains nil attribute" op)
           {:command cmd}))
  (when (nil? v)
    (raise :transact-exception/nil-v
           (format "%s contains nil value" op)
           {:command cmd})))

(defn ->cmd
  "Creates a validated add or retract command containing resolved e, a, v, and
   the corresponding attribute object"
  [construct-fn db cmd]
  (validate-command-syntax! (seq cmd))
  (let [[op e a v] (seq cmd)
        e          (->valid-entity-id db e)
        attr       (attr/resolve-attribute db (entid-strict db a))
        vt         (attr/value-type attr)
        v'         (cast-value db vt v)]
    (construct-fn e
                  (entid-strict db a)
                  v'
                  attr)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Adds

;; A fully resolved and validated add operation 
(defrecord Add [e ^Long a v ^Attribute attr]
  Object
  (toString [_]
    (format "[:db/add %s %s %s]" e (attr/ident attr) v)))
(defmethod print-method Add
  [^Add a ^java.io.Writer w]
  (.write w "[:db/add ")
  (print-method (.e a) w)
  (.write w " ")
  (print-method (.a a) w)
  (.write w " ")
  (print-method (.v a) w)
  (.write w "]"))
(defn add? [o] (instance? Add o))

(defn add-op? [o]
  (or (= :db/add o)
      (= ":db/add" o)))

(def ->add (partial ->cmd ->Add))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Retracts

;; A fully resolve and validated retract operation
(defrecord Retract
  [e ^Long a v ^Attribute attr]
  Object
  (toString [_]
    (format "[:db/retract %s %s %s]" e (attr/ident attr) v)))
(defmethod print-method Retract
  [^Retract r ^java.io.Writer w]
  (.write w "[:db/retract ")
  (print-method (.e r) w)
  (.write w " ")
  (print-method (.a r) w)
  (.write w " ")
  (print-method (.v r) w)
  (.write w "]"))
(defn retract? [o] (instance? Retract o))

(defn retract-op? [o]
  (or (= :db/retract o)
      (= ":db/retract" o)))

(def ->retract (partial ->cmd ->Retract))

(defn ->final-retract [db e a v attr]
  (->Retract e a v attr))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common

(defn pack [cmd]
  (condp instance? cmd
    Add     [(:e cmd)              (:a cmd) (:v cmd)]
    Retract [(bit-set (:e cmd) 62) (:a cmd) (:v cmd)]))
