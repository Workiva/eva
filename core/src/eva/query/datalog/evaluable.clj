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

(ns eva.query.datalog.evaluable
  (:require [eva.query.datalog.protocols :as p]
            [eva.query.datalog.error :refer [raise-evaluable-error]]
            [utiliva.core :refer [keep]]
            [clojure.core.unify :as u])
  (:refer-clojure :exclude [keep]))

(defmacro handling-f
  [& body]
  `(try ~@body
        (catch Throwable t#
            (raise-evaluable-error "" {} t#))))

(defn scalar-bind-fn->Evaluable
  "Assumes that f returns a single scalar value for each call. This produces
  an Evaluable implementation that unified this return value with the final term
  of each group of terms passed in."
  [f args-count]
  (letfn [(eval [terms]
            (let [[args [binds]] (split-at args-count terms)
                  res (handling-f (apply f args))]
              (when (and (some? res) (or (u/lvar? binds) (= res binds)))
                (conj (vec args) res))))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (keep eval terms)))))

(defn batched-scalar-bind-fn->Evaluable
  [f args-count]
  (letfn [(->args-binds [terms]
            (let [[args [binds]] (split-at args-count terms)]
              [args binds]))
          (eval [terms]
            (let [[args binds] (apply map list (map ->args-binds terms))
                  res (handling-f (f args))]
              (map (fn [res args binds]
                     (when (and (some? res) (or (u/lvar? binds) (= res binds)))
                       (conj (vec args) res)))
                   res
                   args
                   binds)))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (keep identity (eval terms))))))

(defn coll-bind-fn->Evaluable
  "Assumes that f returns a collection of results for each call. This produces
  an Evaluable implementation that unifies in turn each item of this collection with
  the last term of each group of terms passed in."
  [f args-count]
  (letfn [(eval [terms]
            (let [[args binds] (split-at args-count terms)
                  res (handling-f (apply f args))]
              (keep #(when (and % (u/unify [%] binds))
                       (vec (concat args [%])))
                    res)))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (mapcat eval terms)))))

(defn tuple-bind-fn->Evaluable
  "Assumes that f returns a collection of results for each call. This produces an
  Evaluable implementation that unifies that collection with the final
  =(- terms-count args-count)= terms of each group of terms passed in."
  [f args-count]
  (letfn [(eval [terms]
            (let [[args binds] (split-at args-count terms)
                  res (handling-f (apply f args))]
              (when (and (some? res) (u/unify res binds))
                (vec (concat args res)))))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (keep eval terms)))))

(defn relation-bind-fn->Evaluable
  "Assumes that f returns a collection of relations for each call. This produces
  an Evaluable implementation that unifies in turn each relation with the final
  =(- terms-count args-count)= terms of each group of terms passed in."
  [f args-count]
  (letfn [(eval [terms]
            (let [[args binds] (split-at args-count terms)
                  res (handling-f (apply f args))]
              (keep #(when (and % (u/unify % binds))
                       (vec (concat args %)))
                    res)))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (mapcat eval terms)))))

(defn pred-fn->Evaluable
  "Assumes that f returns a truthy/falsy value for each call. This produces
  an Evaluable implementation that returns each group of terms passed in for which
  f returns a truthy value."
  [f]
  (letfn [(eval [terms] (when (handling-f (apply f terms)) terms))]
    (reify p/Evaluable
      (evaluations [_ terms]
        (keep eval terms)))))

(defn batched-pred-fn->Evaluable
  "Assumes that f returns a truthy/falsy value for each call. This produces
  an Evaluable implementation that returns each group of terms passed in for which
  f returns a truthy value."
  [f]
  (letfn [(eval [terms] (keep #(when %2 %) terms (handling-f (f terms))))]
    (reify p/Evaluable
      (evaluations [_ terms] (eval terms)))))
