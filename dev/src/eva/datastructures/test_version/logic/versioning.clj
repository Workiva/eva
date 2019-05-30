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

(ns eva.datastructures.test-version.logic.versioning
  (:require [eva.datastructures.test-version.logic.nodes :as nodes]
            [eva.datastructures.test-version.logic.buffer :as buffer]
            [eva.datastructures.test-version.logic.message :as message]
            [eva.datastructures.test-version.logic.protocols :as pr :refer [VERSION]]
            [eva.v2.datastructures.bbtree.logic.v0.nodes :as v0-nodes]
            [eva.v2.datastructures.bbtree.logic.v0.buffer :as v0-buffer]
            [eva.v2.datastructures.bbtree.logic.v0.message :as v0-message]
            [eva.v2.datastructures.bbtree.logic.v0.protocols :as v0-pr]
            [utiliva.core :refer [map-vals]]
            [eva.datastructures.versioning :refer [version-conversion]])
  (:import [eva.datastructures.test_version.logic.buffer BTreeBuffer]))

(defmulti convert-from-v0
  "Dispatches based on object type."
  (fn [o] (type o)))

(defmethod convert-from-v0
  :default
  convert-from-v0:undefined
  [o] (throw (IllegalArgumentException. (format "Conversion from %s is undefined." (type o)))))

(defn convert-children
  [children]
  (let [ks (keys children)]
    (-> (reduce #(assoc % %2
                        (convert-from-v0 (get children %2))) ;; <== recursive
                children
                ks))))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.nodes.BufferedBTreeNode
  convert-from-v0:node
  [o]
  (let [updated-children (if (v0-pr/leaf-node? o)
                           (v0-pr/children o)
                           (convert-children (v0-pr/children o)))
        updated-buffer (convert-from-v0 (v0-pr/messages o))]
    (-> (nodes/map->BufferedBTreeNode o)
        (pr/children updated-children)
        (pr/messages updated-buffer))))

;; POINTER CONVERSION MISSING
;; IT IS IN eva.datastructures.test-version.logic.missing-versioning

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.buffer.BTreeBuffer
  convert-from-v0:buffer
  [^eva.v2.datastructures.bbtree.logic.v0.buffer.BTreeBuffer o]
  (let [v0-mail (.mailboxes o)
        cnt (.cnt o)
        order (.order o)
        _meta (._meta o)
        mailboxes (into (empty v0-mail)
                        (map-vals (partial mapv convert-from-v0))
                        v0-mail)]
    (BTreeBuffer. mailboxes cnt order _meta)))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.BTreeMessage
  convert-from-v0:generic-message
  [o] (message/map->BTreeMessage o))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.UpsertMessage
  convert-from-v0:upsert-message
  [o] (message/map->UpsertMessage o))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.DeleteMessage
  convert-from-v0:delete-message
  [o] (message/map->DeleteMessage o))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.DeleteMessage
  convert-from-v0:delete-message
  [o] (message/map->DeleteMessage o))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.FilterMessage
  convert-from-v0:filter-message
  [o] (message/map->FilterMessage o))

(defmethod convert-from-v0
  eva.v2.datastructures.bbtree.logic.v0.message.RemoveIntervalMessage
  convert-from-v0:remove-interval-message
  [o] (message/map->RemoveIntervalMessage o))

(defmethod version-conversion
  [VERSION v0-pr/VERSION]
  ensure-version:v0->test-version
  [_ o _]
  (convert-from-v0 o))
