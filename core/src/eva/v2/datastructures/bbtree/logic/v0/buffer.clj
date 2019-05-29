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

(ns eva.v2.datastructures.bbtree.logic.v0.buffer
  "A map-like structure with the following quirks:
    - each key is associated with a vector of all values inserted at that key.
    - `count' returns the sum of lengths of all vector values.
    - it works as a priority-map, sorting each key by the length of its associated vector.
    - Corollary: `first' returns the entry with the longest value vector."
  (:require [eva.v2.datastructures.bbtree.logic.v0.protocols :refer [recip ranged?]]
            [eva.error :refer [insist]]
            [potemkin :as p])
  (:import (clojure.lang SeqIterator)))

(p/def-map-type BTreeBuffer
  [mailboxes cnt order _meta]
  (get [_ k default-value] (get mailboxes k default-value))
  (assoc [_ k v]
         ;; Use 'insert' to add individual messages. Use 'assoc' to add a vector of messages.
         (insist (and (nil? (get mailboxes k)) (vector? v))
                 "BTreeBuffer: Use 'insert' to add individual messages. Use 'assoc' to add a vector of messages.")
         (BTreeBuffer. (assoc mailboxes k v) (+ cnt (count v)) order _meta))
  (dissoc [_ k]
          (let [c (count (get mailboxes k))]
            (BTreeBuffer. (dissoc mailboxes k) (- cnt c) order _meta)))
  (keys [_] (keys mailboxes))
  (meta [_] _meta)
  (with-meta [_ mta] (BTreeBuffer. mailboxes cnt order mta))
  clojure.lang.IPersistentMap
  (empty [_] (BTreeBuffer. {} 0 order {}))
  Iterable
  (iterator [this] (SeqIterator. this))
  clojure.lang.Counted
  (count [this] cnt)
  clojure.lang.IFn
  (invoke [this k] (get this k)))

(defn overflowing? [buffer] (> (count buffer) (.order ^BTreeBuffer buffer)))
(defn insert [^BTreeBuffer buffer k v]
  (BTreeBuffer. (update (.mailboxes buffer) k (fnil conj []) v)
                (inc (.cnt buffer))
                (.order buffer)
                (._meta buffer)))

(defn keys-to-flush
  [buffer]
  (let [target (- (count buffer) (.order ^BTreeBuffer buffer))]
    (loop [c 0
           ks nil
           msgs (sort-by (comp count val) > buffer)]
      (if (and (< c target) msgs)
        (recur (+ c (count (val (first msgs))))
               (conj ks (key (first msgs)))
               (next msgs))
        ks))))

;; TODO: rename the following two functions to something more informative.
(defn get-seq
  "Given a buffer and a sequence of keys, returns a concatenated list of all messages
  bound for those keys."
  [buffer & ks]
  (apply concat (vals (select-keys buffer ks))))

(defn get-all
  "Return a sequence of all messages in the mailboxes."
  [buffer]
  (apply concat (vals buffer)))

(defn min-recip
  [min buffer]
  (when-let [ks (keys buffer)]
    (let [notable-msgs (->> (apply min ks) (get buffer) (filter (complement ranged?)))]
      (when-not (empty? notable-msgs)
        (->> notable-msgs (map recip) (apply min))))))

(defn max-recip
  [max buffer]
  (when-let [ks (keys buffer)]
    (let [notable-msgs (->> (apply max ks) (get buffer) (filter (complement ranged?)))]
      (when-not (empty? notable-msgs)
        (->> notable-msgs (map recip) (apply max))))))

(defn- inside-range?
  [^java.util.Comparator cmp x [y1 y2]]
  (and (<= (.compare cmp (recip x) y2) 0)
       (>= (.compare cmp (recip x) y1) 0)))

(defn get-range-filtered
  [buffer cmp k ranges]
  (filter #(some (partial inside-range? cmp %) ranges)
          (get buffer k)))

(defn btree-buffer
  [order]
  (BTreeBuffer. {} 0 order {}))
