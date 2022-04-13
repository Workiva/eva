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

(ns eva.entity
  (:require [eva.attribute :as attr]
            [eva.core :as core]
            [eva.error :refer [insist]]
            [clojure.edn])
  (:import (clojure.lang Seqable Associative ILookup IFn MapEntry)
           (eva Database)))

(declare entity ->Entity touch-entity touch-components datoms->cache equiv-entity lookup-entity entity-cache entity-touched?)

(defn entity [^Database db eid]
  ;; TODO: check if valid db
  ;; TODO: check if valid eid
  (when-some [eid (.entid db eid)]
    (->Entity db eid (atom {:touched? false
                            :cache    {}}))))

(deftype ^:internal Entity [db eid state]
  Object
  (toString [_] (pr-str (assoc (:cache @state) :db/id eid)))
  (hashCode [_] (bit-xor (hash db) (hash eid)))
  (equals [this other] (equiv-entity this other))
  eva.Entity
  (db [_] db)
  (get [this k] (.valAt this k))
  (touch [this] (touch-entity this))
  (keySet [this]
    (touch-entity this)
    (keys (:cache @state)))
  Seqable
  (seq [this] (touch-entity this) (seq (:cache @state)))
  Associative
  (equiv [this o] (equiv-entity this o))
  (containsKey [this k] (lookup-entity this k))
  (entryAt [this k] (some->> (lookup-entity this k) (MapEntry. k)))
  (empty [_] (throw (UnsupportedOperationException.)))
  (assoc [_ _ _] (throw (UnsupportedOperationException.)))
  (cons [_ _] (throw (UnsupportedOperationException.)))
  (count [this] (touch-entity this) (count (:cache @state)))
  ILookup
  (valAt [this k] (lookup-entity this k))
  (valAt [this k not-found] (or (lookup-entity this k) not-found))
  IFn
  (invoke [this k] (lookup-entity this k))
  (invoke [this k not-found] (or (lookup-entity this k) not-found)))

(defn ^:internal entity? [x] (instance? eva.Entity x))

(defn ^:internal equiv-entity [this that]
  (and (entity? this)
       (entity? that)
       (= (.db ^eva.Entity this) (.db ^eva.Entity that))
       (= (.get ^eva.Entity this :db/id) (.get ^eva.Entity that :db/id))))

(defn ^:internal read-keyword [kw]
  (cond (keyword? kw) kw
        (string? kw) (recur (clojure.edn/read-string kw))
        :else (throw (IllegalArgumentException. (format "cannot read keyword from: " kw)))))

(defn ^:internal reverse-ref? [attr]
  (cond (keyword? attr) (= \_ (nth (name attr) 0))
        (string? attr) (recur (read-keyword attr))
        :else
        (throw (IllegalArgumentException. (str "bad attribute type: " attr ", expected keyword or string")))))

(defn ^:internal reverse-ref [attr]
  (cond (keyword? attr) (if (reverse-ref? attr)
                          (keyword (namespace attr) (.substring (name attr) 1))
                          (keyword (namespace attr) (str "_" (name attr))))
        (string? attr) (recur (read-keyword attr))
        :else
        (throw (IllegalArgumentException. (str "bad attribute type: " attr ", expected keyword or string")))))

(defn ^:internal -lookup-backwards
  "Given db, entity-id, attribute, and optional not-found value,
  returns the set of datoms in the database that match the following conditions:
  1) match the pattern [_ attribute entity-id _]
  2) attribute has type :db.value/ref"
  [db eid attr]
  (insist (attr/ref-attr? db attr) (format "reverse lookup failure: attribute %s is not of type ref" attr))
  (core/select-datoms db [:vaet eid attr]))

(defn ^:internal -lookup-forwards
  "Given db, entity-id, attribute, and optional not-found value,
  returns the set of datoms in the database that match the following conditions:
  1) match the pattern [entity-id attribute _ _]"
  [db eid attr]
  (core/select-datoms db [:eavt eid attr]))

(defn ^:internal -lookup-eid
  "Given db and entity-id,
  returns the datoms in the database that match the following conditions:
  1) match the pattern [entity-id _ _ _]
  If not datoms match, nil is returned."
  [db eid]
  (core/select-datoms db [:eavt eid]))

(defn ^:internal entity-attr [db a datoms]
  (if (attr/card-many? db a)
    (if (attr/ref-attr? db a)
      (reduce #(conj %1 (entity db (:v %2))) #{} datoms)
      (reduce #(conj %1 (:v %2)) #{} datoms))
    (if (attr/ref-attr? db a)
      (entity db (:v (first datoms)))
      (:v (first datoms)))))

(defn ^:internal entity-touched? [^Entity e] (boolean (:touched? @(.-state e))))

(defn ^:internal touch-entity [^Entity e]
  {:pre [(entity? e)]}
  (let [db (.-db e)
        eid (.-eid e)
        entity-state (.-state e)]
    (swap! entity-state
           (fn [{:as state :keys [touched?]}]
             (if touched?
               state
               (if-some [datoms (not-empty (-lookup-eid db eid))]
                 (assoc state
                        :cache (->> datoms
                                    (datoms->cache (.-db e))
                                    (touch-components (.-db e)))
                        :touched? true)
                 (assoc state :cache {} :touched? true))))))
  e)

(defn ^:internal entity-cache [^Entity entity] (when-some [state (.-state entity)] (:cache @state)))

(defn ^:internal lookup-entity
  ([^Entity this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (let [attr (read-keyword attr)]
     (if (= attr :db/id)
       (.-eid this)
       (if (reverse-ref? attr)
         (-lookup-backwards (.-db this) (.-eid this) (reverse-ref attr))
         (or (get (entity-cache this) attr)
             (if (entity-touched? this)
               not-found
               (if-let [datoms (not-empty (-lookup-forwards (.-db this) (.-eid this) attr))]
                 (let [value (entity-attr (.-db this) attr datoms)]
                   (swap! (.-state this) assoc-in [:cache attr] value)
                   value)
                 not-found))))))))

(defn ^:internal touch-components [db a->v]
  (letfn [(ensure-entity [db x] (if (entity? x) x (entity db x)))]
    (reduce-kv (fn [acc a v]
                 (assoc acc a
                        (if (attr/is-component? db a)
                          (if (attr/card-many? db a)
                            (set (map (comp touch-entity (fn [x] (ensure-entity db x))) v))
                            (touch-entity (ensure-entity db v)))
                          v)))
               {} a->v)))

(defn ^:internal datoms->cache [^Database db datoms]
  (reduce (fn [acc part]
            (let [a (.identStrict db (:a (first part)))]
              (assoc acc a (entity-attr db a part))))
          {} (vals (group-by :a datoms))))
