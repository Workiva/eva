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

(ns eva.v2.transaction-pipeline.type.tx-fn
  "Defines a concrete type for transaction functions handled in the transaction pipeline."
  (:require [eva
             [attribute :refer [resolve-attribute
                                value-type
                                cardinality
                                card-many?
                                ref-attr?]]
             [bytes :refer [ensure-bytes-wrapped]]
             [core :refer [select-datoms]]
             [entity-id :refer [entid-strict
                                temp?]]
             [utils :refer [one]]]
            [eva.v2.transaction-pipeline.protocols :refer [coerce-to-command]]
            [eva.v2.transaction-pipeline.error :as tpe]
            [recide.core :refer [try*]]
            [eva.core :refer [db-fn? ->fn]])
  (:import (java.util List Map)
           (eva Database)
           (eva.attribute Attribute)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transaction Functions

(defrecord TxFn [^Database db ^List raw f args])

(defn- name-of-fn
  [tx-fn]
  (or (-> tx-fn :f :fn-name) 'name-your-tx-fns-and-this-gets-better))

(defn eval-tx-fn
  "Evaluate the given transaction function, yielding a new coll of tx-data"
  [tx-fn]
  (map #(try* (coerce-to-command % (:db tx-fn))
              (catch :transaction-pipeline/unrecognized-command e
                (tpe/raise-tx-fn-error :tx-fn-illegal-return
                                       "aborting transaction."
                                       {:fn (name-of-fn tx-fn)}
                                       e)))
       (try
         (apply (:f tx-fn) (:db tx-fn) (:args tx-fn))
         (catch Throwable t
           (tpe/raise-tx-fn-error :tx-fn-threw
                                  "aborting transaction."
                                  {:fn (name-of-fn tx-fn)}
                                  t)))))

(defn tx-fn? [o] (instance? TxFn o))

(defn db-fn-op? [db o]
  (db-fn? db o))

(defn ->tx-fn [db cmd]
  (let [[op & args] (seq cmd)]
    (->TxFn db cmd (->fn db op) args)))
