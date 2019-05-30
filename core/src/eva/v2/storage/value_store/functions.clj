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

(ns eva.v2.storage.value-store.functions
  (:require [clojure.data.fressian :as fressian]
            [eva.v2.storage.core :as storage
             :refer [storage-namespace storage-id attributes value]]
            [eva.v2.storage.error :refer [raise-npe raise-data-err]]
            [recide.sanex :as sanex]
            [eva.error :refer [insist]])
  (:import [eva ByteString]
           [java.util UUID]
           [java.nio ByteBuffer BufferUnderflowException]
           [java.io ByteArrayOutputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]
           [org.fressian.impl ByteBufferInputStream]))

(defrecord StorageBlob [block-store namespace id attrs val]) ;; block proxy

;; ======================================================================
;; TYPE NOISE
;; ======================================================================

(defn byte-string->bytebuffer
  [blob]
  (insist (instance? ByteString (:val blob)))
  (update blob :val #(.toByteBuffer ^ByteString %)))

(defn ensure-bytebuffer
  [blob]
  (let [blob (if (instance? ByteString (:val blob))
               (byte-string->bytebuffer blob)
               blob)]
    (if (instance? ByteBuffer (:val blob))
      blob
      (raise-data-err "I don't know how to ensure-bytebuffer."
                      {:invalid (:val blob),
                       ::sanex/sanitary? false})))) ;; customer data

(defn bytebuffer->byte-string
  [blob]
  (insist (instance? ByteBuffer (:val blob)) "bytebuffer->byte-string expects a ByteBuffer.")
  (update blob :val #(ByteString/copyFrom ^ByteBuffer %)))

(defn blob->block
  [blob]
  (storage/->Block (:namespace blob)
                   (:id blob)
                   (:attrs blob)
                   (:val blob)))

;; ======================================================================
;; FRESSIAN SERIALIZATION
;; ======================================================================

(defn- fressian-serializer
  [handlers]
  (fn [input]
    (when (nil? input)
      (raise-npe "Fressian serializer was passed nil."
                 {:method 'fressian-serializer,
                  ::sanex/sanitary? true}))
    (fressian/write input :handlers handlers)))

(defn serialize-gen
  [handlers]
  (let [serializer (fressian-serializer handlers)]
    (fn [blob] (update blob :val serializer))))

(defn- fressian-deserializer
  [handlers]
  (fn [input] (fressian/read input :handlers handlers)))

(defn deserialize-gen
  [handlers]
  (let [deserializer (fressian-deserializer handlers)]
    (fn [blob] (update blob :val deserializer))))

;; ======================================================================
;; ENCRYPTION
;; ======================================================================

(defn encrypt [blob] (update blob :val storage/*encrypt-byte-string*))
(defn decrypt [blob] (update blob :val storage/*decrypt-byte-string*))

;; ======================================================================
;; GZIPPING A BYTEBUFFER
;; ======================================================================

(defn- bytebuffer-gzip
  [^ByteBuffer input]
  (let [baos (ByteArrayOutputStream.)
        gzipper (GZIPOutputStream. baos)]
    (if (.hasArray input)
      (.write gzipper (.array input))
      (.write gzipper (let [ba (byte-array (.remaining input))]
                        (.get input ba)
                        ba)))
    (.close gzipper)
    (ByteBuffer/wrap (.toByteArray baos))))

(defn zip
  [blob]
  (update blob :val bytebuffer-gzip))

(defn- bytebuffer-gunzip
  [^ByteBuffer input]
  (with-open [baos (ByteArrayOutputStream.)
              gunzipper (GZIPInputStream. (ByteBufferInputStream. input))]
    (let [buffer (byte-array 4096)]
      (loop [r (.read gunzipper buffer 0 4096)]
        (when (> r 0)
          (.write baos buffer 0 r)
          (recur (.read gunzipper buffer 0 4096))))
      (ByteBuffer/wrap (.toByteArray baos)))))

(defn unzip
  [blob]
  (update blob :val bytebuffer-gunzip))

;; ======================================================================
;; SHARDING
;; ======================================================================

(defn sharded? [blob]
  (let [compound (get-in blob [:attrs :compound])]
    (boolean (and compound
                  (not (zero? compound))))))

(defn sharded-comparator
  [a b]
  (let [a (sharded? a) b (sharded? b)]
    (cond (and a (not b)) -1
          (and b (not a)) 1
          :else 0)))

(defn too-large?
  [max-size]
  (fn [^StorageBlob blob]
    (cond (instance? ByteString (:val blob))
          (> (.size ^ByteString (:val blob)) max-size)
          (instance? ByteBuffer (:val blob))
          (> (.remaining ^ByteBuffer (:val blob)) max-size))))

(defn- partition-byte-buffer*
  "Partitions a bytebuffer into no more than size n. Returns a lazy sequence."
  [n ^ByteBuffer bb]
  (lazy-seq
   (when (.hasRemaining bb)
     (let [len (min (.remaining bb) n)
           arr (byte-array len)]
       (.get bb arr) ;; Javaland mutation.
       (cons (ByteBuffer/wrap arr)
             (partition-byte-buffer* n bb))))))

;; This piece of indirection is probably unnecessary, but hey. Heisenberg
;; Mutation is the stupidest idea in the history of mutation.
(defn partition-byte-buffer
  "Partitions a bytebuffer into smaller bytebuffers each of length no more
  than n. Returns a lazy sequence."
  [n ^ByteBuffer bb]
  (partition-byte-buffer* n (.asReadOnlyBuffer bb)))

(defn seq-and-tag-shard-addresses
  [[blob]]
  (let [addrs (:val blob)
        cnt (count addrs)]
    (map-indexed (fn [idx addr]
                   (-> blob
                       (assoc-in [:attrs :shard-size] cnt)
                       (assoc-in [:attrs :shard-elt] idx)
                       (assoc-in [:attrs :tag] (:id blob))
                       (assoc :id addr)
                       (assoc :val nil)))
                 addrs)))

(defn completes-shard
  [blob-a blob-b]
  (= (get-in blob-a [:attrs :shard-elt])
     (dec (get-in blob-a [:attrs :shard-size]))))

(defn- gen-shard-ids
  [n k]
  (repeatedly n #(str (UUID/randomUUID))))

(defn shard
  "Shards a block"
  [max-size]
  (fn [blob]
    (let [shards (partition-byte-buffer max-size (:val blob))
          new-keys (gen-shard-ids (count shards) (:id blob))
          new-blob (-> blob
                       (assoc :val new-keys)
                       (update-in [:attrs :compound] (fnil inc 0)))]
      (cons new-blob
            (for [[k s] (zipmap new-keys shards)]
              (-> blob
                  (assoc :id k)
                  (assoc :val s)))))))

(defn join-byte-buffers
  [byte-buffers]
  (insist (every? (partial instance? ByteBuffer) byte-buffers)
          "join-byte-buffers passed wrong type!")
  (let [capacity (reduce + (map #(.remaining ^ByteBuffer %) byte-buffers))
        bb-out (ByteBuffer/allocate capacity)]
    (doseq [^ByteBuffer bb byte-buffers]
      (.put bb-out bb))
        (.flip bb-out)))

(defn unshard
  [blobs]
  (let [one-blob (first blobs)
        unsharded-bytes (join-byte-buffers (map (comp #(.toByteBuffer ^ByteString %) :val)
                                                blobs))]
    (-> one-blob
        (assoc :val unsharded-bytes)
        (update :attrs dissoc :tag))))

(defn wrap-shell-round-innards
  [[shell innards]]
  (-> shell
      (assoc :val (:val innards))
      (update-in [:attrs :compound] dec)))

;; ======================================================================
;; I/O
;; ======================================================================

(defn fetch-blocks
  [blobs]
  (let [one-blob (first blobs)
        block-store (:block-store one-blob)
        namespace (:namespace one-blob)
        blocks (filter some? (storage/read-blocks-only block-store :read-full namespace (map :id blobs)))]
    (for [[blob block] (map vector blobs blocks)]
      (->StorageBlob block-store
                     (storage/storage-namespace block)
                     (storage/storage-id block)
                     (merge (storage/attributes block)
                            (get blob :attrs))
                     (storage/value block)))))

(defn push-blocks
  [blobs]
  (let [block-store (-> blobs first :block-store)
        blocks (map blob->block blobs)]
    (storage/write-blocks-only block-store :write-full blocks)))
