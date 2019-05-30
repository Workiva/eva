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

(ns eva.admin.graph.alpha
  "Provides a basic implementation for migration handled through a flowgraph"
  (:require [flowgraph.core :as d]
            [flowgraph.edge :as edge]
            [eva.v2.storage.value-store :as vs-api]
            [eva.admin.alpha.traversal :as t]
            [recide.sanex.logging :as logging]
            [utiliva.comparator :as c]))

(defn careful-create-node!
  [store n]
  (let [created? @(vs-api/create-key store (t/k n) (t/v n))]
    (if (true? created?)
      ;; we were able to make the new node without issue
      true
      ;; we failed to create the new node ...
      (let [cur-state @(vs-api/get-value store (t/k n))]
        (if (= (t/v cur-state) (t/v n))
          ;; ... but found the extant node was the same as what we wanted
          (do
            (logging/debugf "Ignoring attempt to create extant key %s with equivalent value." (t/k n))
            true)
          ;; ... but found the extant node was different, which is nonrecoverable.
          (throw (IllegalStateException. (format "Failed to migrate node of type %s with key %s. Found extant node with differing value." (type n) (t/k n)))))))))

(defn ->parent [node] (-> node meta ::parent))

;; Nodes monotonically advance through the following states as the migration runs.
;; In some instances, a node may skip states (Eg, in expanding a node we observe
;; that it has no children that we haven't seen --> we can advance it to the
;; :writable state directly)
(def node-states [;; default unmarked/minimum state.
                  nil
                  ;; we've encountered this node, but have not performed any operations upon it.
                  :seen
                  ;; this node has children which we may or may not have seen.
                  :expandable
                  ;; we've processed this nodes parent and have enqueued its children for writing
                  :expanded
                  ;; all of this nodes children are written
                  :writable
                  ;; this node is written
                  :written])

(def state-priority (zipmap [:written :writable :seen nil :expandable :expanded] (range)))

(def states-set (into #{} node-states))

(def state-order (zipmap node-states (range)))

(defn state-comp [x1 x2] (compare (state-order x1) (state-order x2) ))

(def node-info-comparator (c/compare-comp (fn depth-comp [x1 x2]
                                            (compare (:depth x2) ;; deeper first.
                                                     (:depth x1)))
                                          (fn state-priority-comp [x1 x2]
                                            (compare (-> x1 :state state-priority)
                                                     (-> x2 :state state-priority)))))

(defrecord NodeInfo [depth state node k])

(defprotocol MigrationContext
  (ledger [this k] "The ledger is a map from all node keys that the migration context has seen to the most advanced state we know the node to be in, as per 'node-states'")
  (at-state? [this k state] "Returns true if the node is in the ledger at a state >= the provided state.")
  (note-state [this k state] "Update the node state in the ledger to (max current-state state)")
  (handle-new [this k] "Update the migration context for a node that we expect to be new")
  (handle-seen [this k] "Update the migration context for a node that we expect to be newly seen")
  (handle-written [this k] "Update the migration context to note that the provided node has been written")
  (maybe-writable [this k] "Given a node key, update the ledger to note that it can be written *provided* all of its children are written.")
  (maybe-release-parents [this k] "Given the key of a written node, check the set parents of the node. If any are releasable, remove the nodes from the awaiting-parents and add them as released-parents")
  (process-parent [this parent-node-info parent-k children-ks]
    "If the provided parent node has any children which are not yet written, note them as such and add the node itself to the context to await release once the nodes' children have been written."))

(defrecord MigrationContextImpl [dest-store
                                 ;; \/ {node-key --> state}
                                 node-ledger
                                 ;; \/ {parent-key --> #{child-keys}}
                                 children
                                 ;; \/ {child-key --> #{parent-keys}}
                                 parents
                                 awaiting-parents]
  MigrationContext
  (at-state? [this k state] (c/>= state-comp (ledger this k) state))

  (ledger [this k]
    (get node-ledger k))

  (note-state [this k state]
    (if (at-state? this k state)
      this
      (update this :node-ledger assoc k state)))

  (handle-new [this k] (note-state this k :seen))

  (maybe-release-parents [this k]
    (let [releasable-parents
          (for [pk (get parents k)
                :when (and (every? #(at-state? this % :written) (children pk))
                           (not (at-state? this pk :written)))]
            (get awaiting-parents pk))]
      (as-> this this
        (reduce #(update %1 :awaiting-parents dissoc (:k %2)) this releasable-parents)
        (assoc this :released-parents releasable-parents))))

  (handle-written [this k]
    (as-> this this
      (note-state this k :written)
      (update this :awaiting-parents dissoc k)
      (maybe-release-parents this k)))

  (maybe-writable [this k]
    (if (every? #(at-state? this % :written) (children k))
      (note-state this k :writable)
      this))

  (process-parent [this parent-node parent-k children-ks]
    (let [unwritten-children (remove #(at-state? this % :written) children-ks)]
      (if (empty? unwritten-children)
        ;; we don't need to process this parent -- all of its children are written.
        this
        ;; we need to track the structure of the set of unwritten children.
        (as-> this this
          (note-state this parent-k :expanded)
          (update this :children assoc parent-k (into #{} unwritten-children))
          (update this :awaiting-parents assoc parent-k parent-node)
          (reduce (fn [this child-k]
                    (-> this
                        (note-state child-k :seen)
                        (update-in [:parents child-k] (fnil conj #{}) parent-k)))
                  this unwritten-children))))))

(defn migration-context [store]
  (atom (MigrationContextImpl. store {} {} {} {})))

(defmulti handle-node
  (fn [context [{:as node-info :keys [depth state node]}]]
    (logging/tracef "handling node %s %s %s %s" depth state (t/k node) (type node))
    state))

(defn transitioned? [[ledger-before ledger-after] k state]
  (assert (string? k))
  (and (not (at-state? ledger-before k state))
       (at-state? ledger-after k state)))

(defmethod handle-node nil [context [{:as node-info :keys [depth state node k]}]]
  (let [ledger-vals (swap-vals! context handle-new k)]
    (when (transitioned? ledger-vals k :seen)
      [(assoc node-info :state :seen)])))

(defmethod handle-node :seen [context [{:as node-info :keys [depth state node k]}]]
  (if (t/expandable? node)
    [(assoc node-info :state :expandable)]
    [(assoc node-info :state :writable)]))

(defmethod handle-node :expandable [context [{:as node-info :keys [depth state node k]}]]
  (let [all-children (->> (t/expand node (map #(->NodeInfo (inc depth) :seen % (t/k %))))
                          (doall))
        ledger-vals (swap-vals! context process-parent node-info k (map :k all-children))
        new-children (filter #(transitioned? ledger-vals (:k %) :seen) all-children)]
    (logging/tracef "added %s new child nodes upon expansion of %s" (count new-children) k)
    (if (not-empty new-children)
      new-children
      [(assoc node-info :state :writable)])))

(defmethod handle-node :writable [context [{:as node-info :keys [depth state node k]}]]
  (let [store (-> context deref :dest-store deref)]
    (careful-create-node! store node)
    (logging/tracef "wrote node: %s" k)
    (let [ledger-vals (swap-vals! context handle-written k)]
      (if (and (not-empty (:released-parents (second ledger-vals)))
               (not= (:released-parents (first ledger-vals))
                     (:released-parents (second ledger-vals))))
        (do (logging/tracef "releasing %s parents: %s"
                            (count (:released-parents (second ledger-vals)))
                            (into [] (map :k (:released-parents (second ledger-vals)))))
            (conj (map #(assoc % :state :writable) (:released-parents (second ledger-vals)))
                  (assoc node-info :state :written)))
        [(assoc node-info :state :written)]))))

(defmethod handle-node :written [context [{:keys [node k]}]] []) ;; drop reference to node from graph

(d/deflow migration-graph
  [context]
  (let [count-a (atom 0)]
    {:source (edge/transform (fn [node] (->NodeInfo (swap! count-a dec) nil node (t/k node))) :nodes)
     :nodes (edge/transform (partial handle-node context) :nodes :batching? 1)})
  {:nodes {:priority-queue? node-info-comparator}})
