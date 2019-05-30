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

(ns eva.datastructures.test-version.logic.storage
  (:require [eva.datastructures.test-version.logic.state :as state]
            [eva.datastructures.test-version.logic.nodes :as nodes]
            [eva.datastructures.test-version.logic.protocols :as protocols]
            [eva.v2.datastructures.bbtree.error :refer [raise-storage]]
            [eva.v2.datastructures.bbtree.storage :refer [get-nodes put-nodes get-node uuid]]
            [barometer.core :as em])
  (:import [eva.v2.storage.value_store.protocols IValueStorage]))

(def persist-tree-counter
  (em/get-or-register em/DEFAULT 'eva.datastructures.test-version.logic.storage:persist-tree.counter
                      (em/counter "Counts the number of times that 'persist-tree' is called.")))

(def persist-tree-node-hist
  (em/get-or-register em/DEFAULT 'eva.datastructures.test-version.logic.storage:persist-tree-node.histogram
                      (em/histogram (em/reservoir) "Records the number of nodes persisted at each 'persist-tree' call.")))

(defn persist-tree
  "Takes the IBBTreeSTorageBackend implementation and all new pointer-node pairs. Persists!"
  [store all-pairs]
  (em/increment persist-tree-counter)
  (em/update persist-tree-node-hist (count all-pairs))
  (try (put-nodes store all-pairs)
       (catch Exception e
         (raise-storage :failure "failed to persist the new tree." {:method 'persist-tree} e))))

(defn init-persist
  "When a new map or set is created, this persists a pointer to the empty initial root.
  Returns the persisted node with its new UUID."
  [store node]
  (binding [state/*store* store]
    (let [node-pointer (nodes/node->pointer node)
          node (uuid node (uuid node-pointer))]
      (put-nodes store {node-pointer node})
      node)))
