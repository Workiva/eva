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

(ns eva.query.util
  (:require [utiliva.core :refer [keep zip-to zip-from]]
            [clojure.core.unify :as u])
  (:import [clojure.lang MapEntry])
  (:refer-clojure :exclude [keep]))

(def ^:const genned-lvar-separator "__")

(defn lvar-base-name [^String s]
  (let [separator genned-lvar-separator
        slen (.length s)
        sep-idx (.indexOf s ^String separator)]
    (if (and (< 0 sep-idx (- slen 2)))
      (.substring s 0 sep-idx)
      s)))

(defn lvar-strict?
  [sym]
  (and (symbol? sym)
       (re-matches #"^\?.*" (name sym))))

(defn fresh-lvar [s]
  (if (u/ignore-variable? s)
    s
    (-> s name lvar-base-name (str genned-lvar-separator) gensym)))

(defn simple-unify
  ([terms tuple] (simple-unify terms {} tuple))
  ([terms unification-map tuple]
   (loop [[t & tnext] terms
          [u & unext] tuple
          unification-map (transient unification-map)]
     (when-let [unification-map (cond (or (= t u) (u/ignore-variable? t) (u/ignore-variable? u))
                                      unification-map
                                      (and (lvar-strict? t) (= u (get unification-map t u)))
                                      (assoc! unification-map t u)
                                      (and (lvar-strict? u) (= t (get unification-map u t)))
                                      (assoc! unification-map u t))]
       (if tnext (recur tnext unext unification-map) (persistent! unification-map))))))

(defn unifies? [terms tuple] (boolean (simple-unify terms tuple)))

(defn simple-unifications
  "Returns all unification maps resulting from the cross product of
  c-of-bindings and c-of-targets that also unify with terms."
  [terms c-of-bindings c-of-targets]
  (loop [[term & terms] terms
         umaps->targets (for [umap c-of-bindings
                              target c-of-targets]
                          (MapEntry/create umap target))]
    (let [umaps->targets (sequence
                          (comp (keep (fn [umap->target]
                                        (let [umap (key umap->target)
                                              target (val umap->target)
                                              target-var (first target)]
                                          (cond (or (= term target-var) (u/ignore-variable? term) (u/ignore-variable? target-var))
                                                (MapEntry/create umap (next target))
                                                (and (lvar-strict? term) (= target-var (get umap term target-var)))
                                                (MapEntry/create (assoc umap term target-var) (next target))
                                                (and (lvar-strict? target-var) (= term (get umap target-var term)))
                                                (MapEntry/create (assoc umap target-var term) (next target)))))))
                          umaps->targets)]
      (when (seq umaps->targets)
        (if terms
          (recur terms umaps->targets)
          (into #{}
                (map key)
                umaps->targets))))))
