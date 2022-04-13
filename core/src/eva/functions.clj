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

(ns eva.functions
  (:require [schema.core :as s]
            [eva.core :as core]
            [eva.datom]
            [eva.entity-id]
            [clojure.edn]
            [eva.error :refer [raise]]
            [eva.attribute :as attr]
            [clojure.core.cache :as cache])
  (:import (clojure.lang IFn)))

(defprotocol CompilableFunction
  (->f [obj]))

(declare build-db-fn)

(defn ->read-string [str-or-code]
  (if (string? str-or-code)
    (clojure.edn/read-string {:readers *data-readers*} str-or-code)
    str-or-code))

(defn process-reference [kname args]
    `(~(symbol "clojure.core" (clojure.core/name kname))
      ~@(map #(list 'quote %) args)))

(defmulti compile-db-fn (fn [{:keys [lang]}]
                          {:pre [(some? lang)]}
                          (name lang)))

(defn get-else [db eid attr default]
  (if-not (= :db.cardinality/one (attr/cardinality db attr))
    (raise :query/get-else-on-cmany
           "Cannot use get-else on a :db.cardinality/many attribute."
           {:query [eid attr]})
    (let [q (core/select-datoms db [:eavt eid attr])]
      (if-not (empty? q) (:v (first q)) default))))

(defn ^:private strip-quoting [form]
  (if (and (sequential? form)
           (= 'quote (first form)))
    (second form)
    form))

(defmethod compile-db-fn "clojure"
  [{:as fn-record :keys [params code imports requires fn-name]}]
  (let [code (-> (->read-string code)
                 (strip-quoting))
        imports (-> (->read-string imports)
                    (strip-quoting))
        requires (-> (->read-string requires)
                     (strip-quoting))
        params (-> (->read-string params)
                   (strip-quoting))
        ef-name (if (some? fn-name)
                  (symbol (munge fn-name))
                  (gensym "unnamed-database-function"))
        ef `(fn ~ef-name ~(vec params) ~code)]
    (binding [*ns* *ns*]
      (in-ns (gensym "eva.evaluated-db-fn."))
      (refer-clojure)
      (require '[eva.api :refer [q] :as d])
      (let [do-block `(do
                        ~(when (not-empty requires)
                           (process-reference :require requires))
                        ~(when (not-empty imports)
                           (process-reference :import imports))
                        ~ef)
            f (eval do-block)]
        f))))

(defonce fn-cache (atom (cache/lru-cache-factory {} :threshold 1024)))

(defn ensure-cached-fn [fn-cache dbfn]
  (if (cache/has? fn-cache dbfn)
    (cache/hit fn-cache dbfn)
    (cache/miss fn-cache dbfn (compile-db-fn dbfn))))

(s/defrecord DBFn [lang :- (s/either (s/eq "clojure"))
                   params :- [s/Symbol]
                   code :- s/Str
                   imports :- (s/maybe s/Any)
                   requires :- (s/maybe s/Any)
                   fn-name]
  CompilableFunction
  (->f [this]
    (let [cache' (swap! fn-cache ensure-cached-fn this)]
      (get cache' this)))

  Comparable
  (compareTo [this o]
    (compare (hash this) (hash o)))

  IFn
  #_(for [x (range 21) :let [xs (clojure.string/join " " (map #(str "x" %) (range x)))]] (println (format "(invoke [this %s] ((->f this) %s))" xs xs)))
  (invoke [this] ((->f this)))
  (invoke [this x0] ((->f this) x0))
  (invoke [this x0 x1] ((->f this) x0 x1))
  (invoke [this x0 x1 x2] ((->f this) x0 x1 x2))
  (invoke [this x0 x1 x2 x3] ((->f this) x0 x1 x2 x3))
  (invoke [this x0 x1 x2 x3 x4] ((->f this) x0 x1 x2 x3 x4))
  (invoke [this x0 x1 x2 x3 x4 x5] ((->f this) x0 x1 x2 x3 x4 x5))
  (invoke [this x0 x1 x2 x3 x4 x5 x6] ((->f this) x0 x1 x2 x3 x4 x5 x6))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18))
  (invoke [this x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19] ((->f this) x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12 x13 x14 x15 x16 x17 x18 x19))
  (applyTo [this args] (apply (->f this) args)))

(defn build-db-fn
  ([fn-map] (build-db-fn nil fn-map))
  ([fn-name fn-map]
   {:pre [(map? fn-map)]}
   (assoc
    (map->DBFn (reduce-kv (fn [m k v]
                            (assoc m k (-> v ->read-string strip-quoting))) {} fn-map))
    :fn-name (or (:fn-name fn-map)
                 fn-name))))
