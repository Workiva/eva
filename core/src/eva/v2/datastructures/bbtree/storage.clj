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

(ns eva.v2.datastructures.bbtree.storage
  "Generic get/put-node methods wrapped over the value store protocol.
  Non-version-specific. Pointers and Nodes in any version of the btree should
  implement the NodeStorageInfo protocol and storage should just work."
  (:require [eva.v2.datastructures.bbtree.error :refer [raise-storage overrides]]
            [eva.v2.storage.value-store :as value-store]
            [tesserae.core :as tess]
            [utiliva.core :refer [map-keys]]
            [recide.sanex :as sanex]
            [barometer.core :as em]
            [recide.core :refer [try*]]
            [eva.error :refer [error? override-codes]]
            [eva.config :refer [config-strict]])
  (:import [eva.v2.storage.value_store.protocols IValueStorage]))

(defprotocol NodeStorageInfo
  (uuid [this] [this v] "Returns/sets the uuid that this pointer/uuid uses for storage.")
  (node? [this] "Is this a node compatible with the NodeStorage protocol?")
  (node-pointer? [this] "Is this a pointer compatible with the NodeStorage protocol?"))

(extend-protocol NodeStorageInfo
  Object
  (uuid [this] (throw (IllegalArgumentException. (format "%s does not implement NodeStorageInfo." (type this)))))
  (node? [this] false)
  (node-pointer? [this] false)
  nil
  (uuid [this] (throw (NullPointerException. "nil does not implement NodeStorageInfo.")))
  (node? [this] false)
  (node-pointer? [this] false))

(defprotocol NodeStorage
  (get-nodes [store pointers] "Returns a sequence of nodes correspondingly ordered to the keys by which they were fetched.")
  (get-node [store pointer] "Returns the node stored by pointer k")
  (put-nodes [store pointers->nodes] "If all puts succeed, this returns the keys by which the nodes were stored."))

;; ====================================
;; ========== IMPLEMENTATION ==========
;; ====================================

(def ^:dynamic *timeout-ms* (config-strict :eva.v2.storage.request-timeout-ms))

;; =============
;; == METRICS ==
;; =============

(def get-nodes-counter
  (em/get-or-register em/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.storage:vs-get-nodes.counter
                      (em/counter "Counts the number of times that 'get-nodes' is called on a Value Store implementation.")))
(def get-nodes-histogram
  (em/get-or-register em/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.storage:vs-get-nodes.histogram
                      (em/histogram (em/reservoir) "Tracks the number of nodes passed to 'get-nodes' over a Value Store implementation.")))
(def get-node-counter
  (em/get-or-register em/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.storage:vs-get-node.counter
                      (em/counter "Counts the number of times that 'get-node' is called over a Value Store implementation.")))
(def put-nodes-counter
  (em/get-or-register em/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.storage:vs-put-nodes.counter
                      (em/counter "Counts the number of times that 'put-nodes' is called over a Value Store implementation.")))
(def put-nodes-histogram
  (em/get-or-register em/DEFAULT 'eva.v2.datastructures.bbtree.logic.v0.storage:vs-put-nodes.histogram
                      (em/histogram (em/reservoir) "Tracks the number of nodes passed to 'put-nodes' on a Value Store implementation.")))

;; =========================================
;; == ValueStorage implements NodeStorage ==
;; =========================================

;; Implementation & Delegation details:
(extend-protocol NodeStorage
  IValueStorage
  (get-nodes [store pointers]
    (em/increment get-nodes-counter)
    (em/update get-nodes-histogram (count pointers))
    (try*
     (let [ks (map uuid pointers)
           tessera (value-store/get-values store ks)
           results (deref tessera *timeout-ms* ::timed-out)]
       (if (= results ::timed-out)
         (do (tess/revoke-chain tessera false)
             (raise-storage :timeout
                            "'get-nodes' timed out"
                            {:method 'get-nodes, :timeout-ms *timeout-ms*, ::sanex/sanitary? true}))
         (map #(get results %) ks)))
     (catch (:not error?) e
            (raise-storage :failure "unrecognized exception in get-nodes."
                           {:method 'get-nodes, ::sanex/sanitary? false} e))
     (catch error? e
       (throw (override-codes overrides e)))))
  (get-node [store pointer]
    (em/increment get-node-counter)
    (try*
     (let [tessera (value-store/get-value store (uuid pointer))
           v (deref tessera *timeout-ms* ::timed-out)]
       (if (not= v ::timed-out)
         v
         (do (tess/revoke-chain tessera false)
             (raise-storage :timeout
                            "'get-node' timed out"
                            {:method 'get-node,
                             :timeout-ms *timeout-ms*,
                             ::sanex/sanitary? true}))))
     (catch (:not error?) e
            (raise-storage :failure
                           "unrecognized exception in get-node."
                           {:method 'get-node,
                            ::sanex/sanitary? false} e))
     (catch error? e
       (throw (override-codes overrides e)))))
  (put-nodes [store pointers->nodes]
    (em/increment put-nodes-counter)
    (em/update put-nodes-histogram (count pointers->nodes))
    (try*
     (let [tessera (value-store/put-values store (map-keys uuid pointers->nodes))
           res (deref tessera *timeout-ms* ::timed-out)]
       (if (= res ::timed-out)
         (do (tess/revoke-chain tessera false)
             (raise-storage :timeout
                            "'put-nodes' timed out"
                            {:method 'put-nodes,
                             :timeout-ms *timeout-ms*,
                             ::sanex/sanitary? true}))
         (if (every? true? (vals res))
           (keys pointers->nodes)
           (raise-storage :failure "'put-nodes' failed to write some keys."
                          {:method 'put-nodes,
                           :failed (filter #(not (get res %)) (keys res)),
                           :succeeded (filter #(get res %) (keys res)),
                           ::sanex/sanitary? true}))))
     (catch (:not error?) e
            (raise-storage :failure
                           "unrecognized exception in put-nodes."
                           {:method 'put-nodes,
                            ::sanex/sanitary? false}
                           e))
     (catch error? e
       (throw (override-codes overrides e))))))

;; ===============================
;; == MOCKING AND TESTING TOOLS ==
;; ===============================

;; You can just use an empty (atom {}) as your store:
(extend-protocol NodeStorage
  clojure.lang.Atom
  (get-nodes [store pointers]
    (map @store (map uuid pointers)))
  (get-node [store pointer]
    (get @store (uuid pointer)))
  (put-nodes [store pointers->nodes]
    (swap! store into (map-keys uuid pointers->nodes))
    (keys pointers->nodes)))
