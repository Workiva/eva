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

(ns eva.datastructures.test-version.logic.protocols)

(def VERSION :eva.datastructures.bbtree/test-version)

(defprotocol IBufferedBTreeNode
  ;; manipulation & data
  (new-node-from [this] "Constructs a new node from this one (with a new id), preserving properties =:order=, =:leaf?=, and =:root?=")
  (node-empty [this] "Constructs an empty node, preserving the id and properties =:order=, =:leaf?=, and =:root?=")
  (node-conj [this kv] "Adds a key and value to this node without reference to validity constraints. Updates node description.")
  (node-assoc [this k v] "Adds a key and value to this node without reference to validity constraints. Updates node description.")
  (node-dissoc [this k] "Removes a key from this node without reference to validity constraints. Updates node description.")
  (node-get [this k] "Returns the value stored at this node under key k.")
  (buffer-dissoc [this k] "Removes messages bound to k from the buffer.")
  (children [this] [this m] [this f v] "Returns (or sets) the mappy thing containing the children.")
  (messages [this] [this v] [this f v] "Returns the mappy thing representing the message buffer. Or sets it.")
  (node-key-for [this k] "Returns the key to use for insertion (depending on whether it is leaf or inner).")
  ;; types
  (leaf-node? [this] "Is it a leaf node?")
  (mark-leaf [this b] "Mark it as a leaf node.")
  (inner-node? [this] "Is it an inner node?")
  (root-node? [this] "Is it a root node?")
  (mark-root [this b] "Mark it as a root node.")
  ;; node description
  (properties [this] "Returns a properties map.")
  (node-comparator [this] "Returns the comparator used by the node.")
  (node-order [this] "Returns the order of the node.")
  (buffer-size [this] "Returns the buffer-size of the node.")
  (node-size [this] "Returns the node's size.")
  (max-rec [this] [this v] [this f v] "Single arity gets, double arity sets, triple arity updates.")
  (min-rec [this] [this v] [this f v] "Single arity gets, double arity sets, triple arity updates.")
  (transaction-id [this] [this v] "Returns the transaction id at which this node was created.")
  (node-id [this] [this v] "Returns this node's id, persistent across modifications."))

(defprotocol TreeMessage
  (op [this] "Returns the operation this message intends.")
  (ranged? [this] "Returns true if the message is targeted for a range.")
  (recip [this] "Returns the 'recipient' of the message, the final key to which it tends.")
  (payload [this] "Returns the message payload.")
  (apply-message [this cmp kvstore] "Applies this message to the kvstore. Returns kvstore."))

;; The convenience methods wrapping the storage API:

(defprotocol ICustomSelector
  (label [this] "Returns the label used for this selector -- should uniquely identify it.")
  (apply-internal [this avl-children] "Returns a seq of the children relevant to your search.")
  (apply-leaf [this avl-children] "Returns a seq of the values relevant to your search."))
