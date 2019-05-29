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

(ns eva.datastructures.test-version.fressian
  (:require [eva.datastructures.test-version.logic.nodes :as nodes]
            [eva.datastructures.test-version.logic.buffer :as buffer]
            [eva.datastructures.test-version.logic.message :as message]
            [eva.datastructures.test-version.logic.protocols :as prot]
            [eva.datastructures.test-version.logic.error :refer :all]
            [eva.v2.datastructures.bbtree.storage :as node-storage]
            [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.utils.fressian :as eva-fresh]
            [eva.error :refer [error? insist]]
            [recide.core :refer [update-error]]
            [clojure.data.avl :as avl])
  (:import [eva ByteString]
           [clojure.lang IPersistentVector IPersistentList PersistentHashSet Var]
           [eva.datastructures.test_version.logic.nodes BufferedBTreeNode BufferedBTreePointer]
           [eva.datastructures.test_version.logic.buffer BTreeBuffer]
           [eva.datastructures.test_version.logic.message BTreeMessage DeleteMessage UpsertMessage FilterMessage RemoveIntervalMessage]
           [eva.datastructures.test_version.logic.protocols TreeMessage]
           [eva.datastructures.utils.comparators Comparator]
           [eva.datastructures.utils.interval Interval]
           [eva.datastructures.utils.fressian Autological AutologicalFunction]))

(defn version [string] (format "%s-%s" string "test-version"))

(def bbtree-node-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try
        (let [uuid (.readObject reader)
              node-id (.readObject reader)
              tx (.readObject reader)
              buffer (.readObject reader)
              key-vals (.readObject reader)
              props (nodes/map->NodeProperties (.readObject reader))
              cmp (:comparator props)
              error-msg "fressian node reader received node with invalid "]
          (insist (number? node-id) (str error-msg "node-id"))
          (insist (number? tx) (str error-msg "tx"))
          (insist (instance? BTreeBuffer buffer) (str error-msg "buffer"))
          (insist (instance? Comparator cmp) (str error-msg "comparator"))
          (nodes/->BufferedBTreeNode uuid node-id tx buffer (into (avl/sorted-map-by cmp) key-vals) props))
        (catch Exception e
          (if (error? e :fressian.unreadable/*)
            (throw (update-error e :handler-chain conj :bbtree-node))
            (fresh-read-err :bbtree-node "" {:handler-chain [:bbtree-node]} e)))))))

(def bbtree-node-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer node]
      (let [^BufferedBTreeNode node node]
        (.writeTag writer (version "eva/bbtree-node") 6)
        (.writeObject writer (node-storage/uuid node))
        (.writeObject writer (prot/node-id node))
        (.writeObject writer (prot/transaction-id node))
        (.writeObject writer (prot/messages node))
        (.writeObject writer (seq (prot/children node)))
        (.writeObject writer (into {} (prot/properties node)))))))

(def bbtree-pointer-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try (let [uuid (.readObject reader)
                 node-id (.readObject reader)
                 tx (.readObject reader)
                 properties (.readObject reader)]
             (insist (number? node-id) "fressian pointer reader received pointer with invalid id")
             (insist (number? tx) "fressian pointer reader received pointer with invalid tx")
             (insist (map? properties) "fressian pointer reader received pointer with invalid properties")
             (nodes/->BufferedBTreePointer uuid node-id tx properties))
           (catch Exception e
             (if (error? e :fressian.unreadable/*)
               (throw (update-error e :handler-chain conj :bbtree-pointer))
               (fresh-read-err :bbtree-pointer "" {:handler-chain [:bbtree-node]} e)))))))

(def bbtree-pointer-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer pointer]
      (let [^BufferedBTreePointer pointer pointer]
        (.writeTag writer (version "eva/bbtree-pointer") 4)
        (.writeObject writer (node-storage/uuid pointer))
        (.writeObject writer (prot/node-id pointer))
        (.writeObject writer (prot/transaction-id pointer))
        (.writeObject writer (prot/properties pointer))))))

(def bbtree-buffer-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (let [mailboxes (.readObject reader)
            cnt (.readObject reader)
            order (.readObject reader)]
        (insist (map? mailboxes) "fressian buffer reader received buffer with invalid mailboxes")
        (insist (number? cnt) "fressian buffer reader received buffer with invalid count")
        (insist (number? order) "fressian buffer reader receied buffer with invalid order")
        (try (buffer/->BTreeBuffer mailboxes cnt order {})
             (catch Exception e
               (if (error? e :fressian.unreadable/*)
                 (throw (update-error e :handler-chain conj :bbtree-buffer))
                 (fresh-read-err :bbtree-buffer "" {:handler-chain [:bbtree-buffer]} e))))))))

(def bbtree-buffer-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer myClassInst]
      (.writeTag writer (version "eva/bbtree-buffer") 3)
      (.writeObject writer (.mailboxes ^BTreeBuffer myClassInst))
      (.writeObject writer (.cnt ^BTreeBuffer myClassInst))
      (.writeObject writer (.order ^BTreeBuffer myClassInst)))))

(def bbtree-message-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try (let [tx-added (.readObject reader)
                 op (.readObject reader)
                 target (.readObject reader)
                 content (.readObject reader)]
             (case op
               :upsert (message/->UpsertMessage tx-added target content)
               :delete (message/->DeleteMessage tx-added target content)
               :filter (message/->FilterMessage tx-added target content)
               :remove-interval (message/->RemoveIntervalMessage tx-added target content)))
           (catch Exception e
             (if (error? e :fressian.unreadable/*)
               (throw (update-error e :handler-chain conj :bbtree-message))
               (fresh-read-err :bbtree-message "" {:handler-chain [:bbtree-message]} e)))))))

(def bbtree-message-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer message]
      (.writeTag writer (version "eva/bbtree-message") 4)
      (.writeObject writer (:tx-added message))
      (.writeObject writer (prot/op message))
      (.writeObject writer (prot/recip message))
      (.writeObject writer (prot/payload message)))))

(def all-writers
  {BufferedBTreeNode {(version "eva/bbtree-node") bbtree-node-writer}
   BufferedBTreePointer {(version "eva/bbtree-pointer") bbtree-pointer-writer}
   BTreeBuffer {(version "eva/bbtree-buffer") bbtree-buffer-writer}
   UpsertMessage {(version "eva/bbtree-message") bbtree-message-writer}
   DeleteMessage {(version "eva/bbtree-message") bbtree-message-writer}
   FilterMessage {(version "eva/bbtree-message") bbtree-message-writer}
   RemoveIntervalMessage {(version "eva/bbtree-message") bbtree-message-writer}})

(def all-readers
  {(version "eva/bbtree-node") bbtree-node-reader
   (version "eva/bbtree-pointer") bbtree-pointer-reader
   (version "eva/bbtree-buffer") bbtree-buffer-reader
   (version "eva/bbtree-message") bbtree-message-reader})
