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
;; This file uses implementation from Datascript which is distributed under the
;; Eclipse Public License (EPLv1.0) at https://github.com/tonsky/datascript with
;; the following notice:
;;
;; Copyright © 2014–2018 Nikita Prokopov
;;
;; Licensed under Eclipse Public License
;; -----------------------------------------------------------------------------

(ns eva.entity-id
  "Namespace responsible for the creation, interpretation, and packing of entity ids."
  (:refer-clojure :exclude [partition])
  (:require [schema.core :as s]
            [eva.error :refer [raise insist]]
            [recide.sanex :as sanex]
            [clojure.math.numeric-tower])
  (:import (java.io Writer)
           (eva Id Database)))

(defprotocol IEntityID
  (partition     [eid])
  (temp?         [eid])
  (retracted?    [eid])
  (added?        [eid])
  (n             [eid])
  (->Long        [eid])
  (->EID         [eid]))

(def max-p
  "max value of 'p' passed to pack-entity-id
  <= (2^20)-1"
  (dec (long (Math/pow 2 22))))

(def max-e
  "max value of 'e' passed to pack-entity-id
  <= (2^42)-1"
  (dec (long (Math/pow 2 42))))

(defn pack-entity-id
        "A packed entity ID is 64 bits in the shape:
         [1 1 20 42 ].

         The first bit is a temp-id flag
         The next bit is an added / removed flag
         The next 20 are the partition id
         The next 42 are the entity id

         Arguments:
         p         = partition-id
         e         = entity-id
         retracted = true if entity is retracted
         temp      = true if entity-id is temporary"
        ([p e retracted temp]
         {:pre [(<= 0 p max-p)
                (<= 0 e max-e)]}
         (-> p
             (bit-shift-left 42)
             (bit-or e)
             (cond->
               retracted       (bit-set 62)
               (not retracted) (bit-clear 62))
             (cond->
               temp       (bit-set 63)
               (not temp) (bit-clear 63))))
        ;; "Packs a mostly-packed integer that is only missing the
        ;; add/retract bit flag"
        ([op packed-id]
         (case op
           :db/add (bit-clear packed-id 62)
           :db/retract (bit-set packed-id 62))))

(def base-tx-eid (pack-entity-id 1 0 false false))
(def max-tx-eid (pack-entity-id 1 max-e false false))

(defn tx-eid? [num]
  (and (integer? num)
       (<= base-tx-eid num max-tx-eid)))

(defn tx-num? [num]
  (and (integer? num)
       (<= 0 num max-e)))

(defn ->tx-eid "Takes a tx-num or tx-eid and returns the equivalent tx-eid"
  [tx-foo]
  (cond
    (tx-num? tx-foo)
    (+ tx-foo base-tx-eid)
    (tx-eid? tx-foo)
    tx-foo
    :else
    (raise :invalid-conversion/tx-eid
           "The provided number is not a valid tx-num or tx-eid"
           {:tx-foo tx-foo,
            ::sanex/sanitary? false})))

(defn ->tx-num "Takes a tx-num or tx-eid and returns the equivalent tx-num"
  [tx-foo]
  (cond
    (tx-num? tx-foo)
    tx-foo
    (tx-eid? tx-foo)
    (- tx-foo base-tx-eid)
    :else
    (raise :invalid-conversion/tx-num
           "The provided number is not a valid tx-num or tx-eid"
           {:tx-foo tx-foo,
            ::sanex/sanitary? false})))

(s/defrecord EntityID [temp? :- s/Bool
                       retracted? :- s/Bool
                       partition
                       n :- s/Int]
  Id
  IEntityID
  (partition     [_] partition)
  (temp?         [_] temp?)
  (retracted?    [_] retracted?)
  (added?        [_] (not retracted?))
  (n             [_] n)
  (->EID         [this] this)
  (->Long        [_] (pack-entity-id partition
                                     (if temp? (- n) n)
                                     retracted?
                                     temp?)))

(extend-type Long
  IEntityID
  (partition     [this] (-> (clojure.math.numeric-tower/expt 2 20)
                            dec
                            (bit-shift-left 42)
                            (bit-and this)
                            (bit-shift-right 42)))
  (temp?         [this] (bit-test this 63))
  (retracted?    [this] (bit-test this 62))
  (added?        [this] (not (bit-test this 62)))
  (n             [this] (-> (clojure.math.numeric-tower/expt 2 42)
                            dec
                            (bit-and this)))
  (->Long        [this] this))

(defmethod print-method EntityID [eid ^Writer writer]
  (.write writer
          (format "#db/id[%s %s]" (:partition eid) (:n eid))))

(defn entity-id? [x]
  (satisfies? IEntityID x))

(defn entity-id?* [x]
  (instance? EntityID x))

(defn entid-strict
  "Like (.entid db e), but throws if the resulting output is not an entity-id."
  [^Database db e]
  (let [e' (.entid db e)]
    (if-not (entity-id? e')
      (raise :eva.database/cannot-resolve-entid
             (format "Failed to resolve %s to a valid entity-id." e)
             {:e e,
              ::sanex/sanitary? false})

      e')))

;; the range -1000000 to :cur-n will always be the
;; ids that have been locally allocated, even if
;; they are not used in a transaction.
;; the user-id-map will always be a mapping from
;; some entity id created
(def id-info-base {:user-id-map  {}
                   :cur-n -1000000
                   :max-id nil
                   :tempids nil
                   :tx-eid nil})
(def ^:dynamic *id-info* (atom id-info-base))

(defn set-max-allocated-id!
  "resets the max allocated id to the passed value.
   NOTE: also resets the perm-id mapping"
  [max-id prev-tx-eid]
  (swap! *id-info*
         assoc
         :max-id max-id
         :tx-eid (inc prev-tx-eid)
         :tempids {}))

(defn permify-id* [id-info eid]
  (if (contains? (:tempids id-info) eid)
    ;; the id has already been perm'd
    id-info
    ;; we need to perm it.
    (let [new-n (inc (:max-id id-info))
          perm-id (assoc eid
                    :n new-n
                    :temp? false)]
      (-> id-info
           (update :max-id inc)
           (update :tempids assoc eid perm-id)))))

(defn permify-id
  ([eid]
   (if (or (= (:partition eid) 1)
           (= (:partition eid) :db.part/tx)
           (= (:partition eid) ":db.part/tx"))
     (:tx-eid @*id-info*)
     (get (:tempids (swap! *id-info* permify-id* eid)) eid))))

(defn allocate-temp-id
  "Allocates and returns a new temporary id.

   If we are in a peer context, will count down from :cur-n.
   If we are in a transactor context, will count up from :tx-n.

   Context is determined by the existence of the :tx-n key"
  []
  (let [info (swap! *id-info*
                    (fn [info]
                      (if (:tx-n info)
                        (let [allocated (inc (:tx-n info))]
                          (assoc info :tx-n allocated :allocated allocated :tx-tempid? true))
                        (let [allocated (dec (:cur-n info))]
                          (assoc info :cur-n allocated :allocated allocated :tx-tempid? false)))))]
    (select-keys info [:tx-tempid? :allocated])))

(defn tagged-tempid? [tid]
  (and (temp? tid)
       (<= -1000000 (n tid) -1)))

(defn tempid
  ([partition]
   (let [{:keys [tx-tempid? allocated]} (allocate-temp-id)]
     (cond-> (tempid partition allocated)
       tx-tempid? (assoc :tx-tempid? true))))

  ([partition n]
   (strict-map->EntityID {:temp? true
                          :retracted? false
                          :partition partition
                          :n n})))

(s/defn ^:always-validate eid->long [eid :- EntityID]
  ;; some weirdness here, doesn't convert quite right (glossform eid-map-codec long-codec eid)
  (apply pack-entity-id
         ((juxt partition n retracted? temp?) eid)))

(def perm? (complement temp?))

(defn min-id [id1 id2]
  (if (< (:n id1) (:n id2)) id1 id2))

(defn select-exemplar-id
  "Select an exemplar id between the two ids given the following priority:
   permid > user-tagged tempid > user-genned tempid > tx-genned tempid"
  ([id1 id2]
   (cond
     (nil? id1) id2
     ;; reject conflicting ids
     (and (not (temp? id1)) (not (temp? id2)))
     (raise :transact-exception/merge-conflict
            "Cannot merge two extant permanent ids. This typically happens when two distinct entity-ids either are explicitly or implicitly assigned the same :db/unique attribute-value pair."
            {:ids [id1 id2],
             ::sanex/sanitary? false})

     (and (tagged-tempid? id1) (tagged-tempid? id2) (not= id1 id2))
     (raise :transact-exception/merge-conflict
            "Cannot merge two differently-tagged user-ids. This typically happens when two distinct entity-ids either are explicitly or implicitly assigned the same :db/unique attribute-value pair."
            {:ids [id1 id2],
             ::sanex/sanitary? false})

     ;; select perm
     (not (temp? id1)) id1
     (not (temp? id2)) id2

     ;; select user-tagged temp
     (tagged-tempid? id1) id1
     (tagged-tempid? id2) id2

     ;; select user-genned temp
     (and (not (:tx-tempid? id1))
          (not (:tx-tempid? id2))) (min-id id1 id2)

     (not (:tx-tempid? id1)) id1
     (not (:tx-tempid? id2)) id2

     ;; select wlog tx-genned temp
     :else (min-id id2 id2))))
