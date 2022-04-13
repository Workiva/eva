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
;; This file uses implementation from again which is distributed under the
;; Eclipse Public License (EPLv1.0) at https://github.com/liwp/again with the
;; following notice:
;;
;; Copyright © 2014–2017 Listora, Lauri Pesonen
;;
;; Distributed under the Eclipse Public License either version 1.0 or (at your
;; option) any later version.
;; -----------------------------------------------------------------------------

(ns eva.utils
  (:require [clojure.data.avl :as avl]
            [potemkin :refer [fast-memoize]]
            [eva.builtin]
            [eva.error :as err]
            [eva.functions :as ef]
            [recide.sanex :as sanex]
            [recide.sanex.logging :refer [warn]])
  (:import (clojure.lang IDeref)))

(defn one
  "Returns nil or the first item of a collection.
   If there is more than one item in the collection throws exception."
  [coll]
  (case (count coll)
    0 nil
    1 (first coll)
    (err/raise :unable-to-ensure-zero-or-one
               (format "Encountered %s elements in a call to one. THIS SHOULD NOT HAPPEN." (count coll))
               #_{:coll coll, ::sanex/sanitary? true}
               (sanex/specify-sanitization {:coll coll} true))))

(defn fill
  "Appends a fill value to the end of a vector until it's length is 4."
  [fill-val [c0 c1 c2 c3]]
  [(if (some? c0) c0 fill-val)
   (if (some? c1) c1 fill-val)
   (if (some? c2) c2 fill-val)
   (if (some? c3) c3 fill-val)])

(def init-tx-eid 4398046511104) ;; (pack-entity-id 1 0 false false)
(def init-tx-time (java.util.Date. 0))

(defn init-log-info []
  "Returns the data that is contained in the zeroth log entry for a new database."
  {:cur-tx-eid init-tx-eid
   :tx-inst init-tx-time
   :cur-max-id 1023
   :tx-num 0
   ;; TODO: make a trie type for packed datom sets
   :novelty [[0 3 :db.part/db]
             [0 9 "The default database partition."]
             [0 17 21]
             [0 17 22]
             [0 17 23]
             [0 17 24]
             [0 17 25]
             [0 17 26]
             [0 17 27]
             [0 17 28]
             [0 17 29]
             [0 17 30]
             [0 17 31]
             [0 17 32]
             [0 17 33]
             [0 17 34]
             [0 19 0]
             [0 19 1]
             [0 19 2]
             [0 20 3]
             [0 20 4]
             [0 20 5]
             [0 20 6]
             [0 20 7]
             [0 20 8]
             [0 20 9]
             ;; [0 20 10]
             [0 20 11]
             ;; [0 20 12]
             [0 20 13]
             ;; [0 20 14]
             [0 20 15]
             ;; [0 20 16]
             [0 20 17]
             ;; [0 20 18]
             [0 20 19]
             [0 20 20]
             [0 20 41]
             [1 3 :db.part/tx]
             [1 9 "The reified transaction partition."]
             [2 3 :db.part/user]
             [2 9 "The user transaction partition."]
             [3 3 :db/ident]
             [3 4 31]
             [3 5 35]
             [3 6 39]
             [3 9 "Establishes a keyword identity for the given entity."]
             [4 3 :db/valueType]
             [4 4 32]
             [4 5 35]
             [4 9 "Establishes the type of an attribute."]
             [5 3 :db/cardinality]
             [5 4 32]
             [5 5 35]
             [5 9 "Establishes the cardinality of an attribute, :db.cardinality/one or :db.cardinality/many."]
             [6 3 :db/unique]
             [6 4 32]
             [6 5 35]
             [6 9 "Asserts either :db.unique/identity or :db.unique/value semantics for an attribute."]
             [7 3 :db/isComponent]
             [7 4 26]
             [7 5 35]
             [7 9 "Asserts that entities referenced by this attribute have component semantics."]
             [8 3 :db/noHistory]
             [8 4 26]
             [8 5 35]
             [8 9 "Asserts that this attribute should not be indexed historically. NYI."]
             [9 3 :db/doc]
             [9 4 30]
             [9 5 35]
             [9 9 "A docstring for the given entity."]
             ;; NOTE: db/lang is held inside of a db/fn, so we don't need this
             ;; [10 3 :db/lang]
             ;; [10 4 32]
             ;; [10 5 35]
             [11 3 :db/fn]
             [11 4 34]
             [11 5 35]
             [11 9 "The given entity has database function defined by the given value."]
             ;; NOTE: db-code is intrinsic to a function, we don't need it
             ;;       as its own attribute
             ;; [12 3 :db/code]
             ;; [12 4 30]
             ;; [12 5 35]
             ;; NOTE: index is vacuous in our model
             [13 3 :db/index]
             [13 4 26]
             [13 5 35]
             [13 9 "Specify if the given attribute should be AVET indexed. Vacuous in Eva."]
             ;; NOTE: Arity was a local add that isn't really needed
             ;; [14 3 :db/arity]
             ;; [14 4 22]
             ;; [14 5 35]
             [15 3 :db/txInstant]
             [15 4 25]
             [15 5 35]
             [15 9 "The point-in-time the transactor logged the given transaction entity."]
             [17 3 :db.install/valueType]
             [17 4 32]
             [17 5 36]
             [17 9 "An attribute for installing new value types. Currently does nothing."]
             ;; NOTE: We don't use this endpoint to install functions;
             ;; Any id with a :db/fn attribute is an installed function.
             ;;[18 3 :db.install/function]
             ;;[18 4 32]
             ;;[18 5 36]

             [19 3 :db.install/partition]
             [19 4 32]
             [19 5 36]
             [19 9 "Asserts that the given entity should be installed as a new partition."]
             [20 3 :db.install/attribute]
             [20 4 32]
             [20 5 36]
             [20 9 "Asserts that the given entity should be installed as a new attribute."]
             [21 3 :db.type/double]
             [22 3 :db.type/long]
             [23 3 :db.type/bigint]
             [24 3 :db.type/float]
             [25 3 :db.type/instant]
             [26 3 :db.type/boolean]
             [27 3 :db.type/bytes]
             [28 3 :db.type/uri]
             [29 3 :db.type/uuid]
             [30 3 :db.type/string]
             [31 3 :db.type/keyword]
             [32 3 :db.type/ref]
             [33 3 :db.type/bigdec]
             [34 3 :db.type/fn]
             [35 3 :db.cardinality/one]
             [36 3 :db.cardinality/many]
             [37 3 :db/add]
             [38 3 :db/retract]
             [39 3 :db.unique/identity]
             [40 3 :db.unique/value]

             ;; Fulltext attribute
             [41 3 :db/fulltext]
             [41 4 26]
             [41 5 35]
             [41 9 "Indicates that the given :db.type/string attribute should be fulltext indexed. NYI."]
             [init-tx-eid 15 init-tx-time]

             ;; cas Fn
             [42 3 :db.fn/cas]
             [42 11
              (ef/build-db-fn
               {:lang   "clojure"
                :require '[[eva.builtin]]
                :params '[db e a v-old v-new]
                :code   '(eva.builtin/cas db e a v-old v-new)})]
             ;; retractEntity fn
             [43 3 :db.fn/retractEntity]
             [43 11
              (ef/build-db-fn
               {:lang "clojure"
                :require '[[eva.builtin]]
                :params '[db entity-to-retract]
                :code '(eva.builtin/retract-entity db entity-to-retract)})]]})

(defn ensure-avl-sorted-set-by
  "If col is an AVLMap sorted using comparator comp
  then returns col unchanged.
  Else creates AVLMap sorted by comp from col"
  [comp col]
  (if (and (instance? clojure.data.avl.AVLSet col)
           (= (.comparator ^clojure.lang.Sorted col) comp))
    col
    (into (avl/sorted-set-by comp) col)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Again Variants
;;
;; We want slightly more flexible behavior and logging with
;; the retries macro provided by the `again' library; the below was forked from:
;;  https://github.com/listora/again/blob/master/src/again/core.clj

(defn- sleep
  [delay]
  (Thread/sleep (long delay)))

(defn with-retries*
  ([strategy f]
   ;; Indirection because you can't recur from within a try-catch
   (if-let [[res] (try [(f)]
                       (catch RuntimeException e
                         (throw e))
                       (catch Exception e
                         (when-not (seq strategy)
                           (throw e))))]
     res
     (let [[delay & strategy] strategy]
       (sleep delay)
       (recur strategy f))))

  ([strategy warn-msg f]
   (if-let [[res] (try
                    [(f)]
                    (catch RuntimeException e
                      (throw e))
                    (catch Exception e
                      (when-not (seq strategy)
                        (throw e))))]
     res
     (let [[delay & strategy] strategy]
       (warn warn-msg)
       (sleep delay)
       (recur strategy warn-msg f)))))

(defmacro with-retries
  "Try executing `body`. If `body` throws an Exception, retry
  according to the retry `strategy`.
  A retry `strategy` is a seq of delays: `with-retries` will sleep the
  duration of the delay (in ms) between each retry. The total number
  of tries is the number of elements in the `strategy` plus one. A
  simple retry strategy would be: [100 100 100 100] which results in
  the operation being retried four times (for a total of five tries)
  with 100ms sleeps in between tries. Note: that infinite strategies
  are supported, but maybe not encouraged...
  Strategies can be built with the provided builder fns, eg
  `linear-strategy`, but you can also create any custom seq of
  delays that suits your use case."
  [strategy warn-msg & body]
  `(with-retries* ~strategy ~warn-msg (fn [] ~@body)))
