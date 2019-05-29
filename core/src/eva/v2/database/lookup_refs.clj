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

(ns eva.v2.database.lookup-refs
  (:require
   [eva.core :refer [entry->datoms safe-advance-index-better batch-advance-index] :as core]
   [eva.attribute :as attr]
   [eva.error :refer [raise insist] :as ee]
   [utiliva.core :refer [partition-map]]))


(defn  assert-conformant-lookup-ref [db lookup-ref]
  (when (not= 2 (count lookup-ref))
    (core/raise-entid-coercion
     :malformed-lookup-ref
     "lookup references must be a java.util.List of exactly 2 elements" {:lookup-ref lookup-ref}))
  (let [[a v] lookup-ref]
    (when (nil? a) (core/raise-entid-coercion
                    :malformed-lookup-ref
                    "cannot process lookup reference with nil attribute" {:lookup-ref lookup-ref}))
    (when (nil? v) (core/raise-entid-coercion
                    :malformed-lookup-ref
                    "cannot process lookup reference with nil value" {:lookup-ref lookup-ref}))
    (when-let [attr (attr/resolve-attribute db a)]
      (when-not (attr/unique attr)
        (core/raise-entid-coercion
         :malformed-lookup-ref
         "lookup references are only allowed for unique attributes" {:attr attr :lookup-ref lookup-ref})))
    true))

(defn resolve-lookup-ref [{:keys [lookup-ref-cache]:as db} [a v :as lookup-ref]]
  (core/assert-conformant-lookup-ref db lookup-ref)
  (let [cache @lookup-ref-cache]
    (if (contains? cache lookup-ref)
      (get cache lookup-ref)
      (let [res (core/select-datoms db [:avet a v])]
        (when-let [eid (:e (first res))]
          (core/update-lookup-ref-cache! db {lookup-ref eid})
          eid)))))

(defn resolve-lookup-ref-strict [db [a v :as lookup-ref]]
  (core/assert-conformant-lookup-ref db lookup-ref)
  (if-some [eid (:e (first (core/select-datoms db [:avet a v])))]
    eid
    (raise :lookup-ref/no-such-eid
           "Lookup reference does not have corresponding extant eid"
           {:lookup-ref lookup-ref})))

(defn batch-resolve-lookup-refs [db lookup-refs]
  (doseq [lr lookup-refs] (core/assert-conformant-lookup-ref db lr))
  (let [cur-cache @(-> db :lookup-ref-cache)
        ids (partition-map (partial contains? cur-cache)
                           {true  #(map (partial get cur-cache) %)
                            false #(map (comp :e first) (core/multi-select-datoms-ordered db (cons :avet %)))}
                           lookup-refs)]
    ;; NOTE: stores negative results in the cache too.
    (core/update-lookup-ref-cache! db (zipmap lookup-refs ids))
    ids))

(defn batch-resolve-lookup-refs-strict [db lookup-refs]
  (let [ids (batch-resolve-lookup-refs db lookup-refs)]
    (if-let [lookup-ref (->> (map vector ids lookup-refs)
                             (filter #(nil? (first %)))
                             first
                             last)]
      (raise :lookup-ref/no-such-eid
             "Lookup reference does not have corresponding extant eid"
             {:lookup-ref lookup-ref})
      ids)))
