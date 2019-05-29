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

(ns eva.v2.storage.core
  (:require [eva.v2.storage.error :refer [raise-data-err]]
            [eva.error :refer [insist]]
            [morphe.core :as d]
            [barometer.aspects :refer [concurrency-measured]]
            [barometer.core :as em]
            [schema.core :as s]
            [recide.sanex :as sanex])
  (:import (eva ByteString)
           (clojure.lang IFn)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Block Addressable Storage
;;
;; Block namespaces and ids are non-empty strings.
(s/defschema ID (s/both s/Str (s/pred not-empty)))

(defprotocol StorageBlock
  "Getters/setters for a Block. Because it has more type restrictions than
  a normal record, this provides a convenient way to document/enforce these."
  (storage-namespace [_] [_ ns]
    "Get or set the namespace (String) of this block.")
  (storage-id [_] [_ id]
    "Get or set the id (String) of this block.")
  (attributes [_] [_ attribute] [_ attribute val]
    "Get all attributes, a single attribute, or set a single attribute in this Block's attribute map.")
  (value [_] [_ v-or-f]
    "Get or set or update the value (ByteString) of this Block."))

(s/defrecord Ptr [namespace :- ID, id :- ID])

(s/defn ptr :- Ptr
  ([block :- (s/protocol StorageBlock)] (apply ptr ((juxt storage-namespace storage-id) block)))
  ([namespace :- ID, id :- ID] (->Ptr namespace id)))

;; Blocks are the fundamental unit of storage.
(s/defrecord Block [namespace :- ID
                    id :- ID
                    attrs :- {s/Keyword s/Any}
                    val :- (s/maybe ByteString)]
  StorageBlock
  (storage-namespace [_] namespace)
  (storage-namespace [this ns] (assoc this :namespace ns))
  (storage-id [_] id)
  (storage-id [this new-id] (assoc this :id new-id))
  (attributes [_] attrs)
  (attributes [_ attribute] (get attrs attribute))
  (attributes [this attribute val] (update-in this [:attrs attribute] val))
  (value [_] val)
  (value [this v]
    (cond (ifn? v) (->> this :val v (value this))
          (instance? ByteString v) (assoc this :val v)
          (nil? v) (assoc this :val v)
          :else (raise-data-err "Block values must be ByteStrings or nil."
                                {:invalid v,
                                 ::sanex/sanitary? false}))))

(defn simple-block? [block] (not (attributes block :compound)))
(defn compound-block? [block] (boolean (attributes block :compound)))

(def ^:dynamic *max-block-size*
  "By default Blocks have a maximum size of 64 KB"
  (* 64 1024))

;; Block Encryption/Decryption
(s/defn ^:dynamic *encrypt-byte-string* :- ByteString
  "Dynamic function used to encrypt the individual block vals. Default implementation
  is a no-op; rebind this function to provide actual encryption logic."
  [v :- ByteString] v)
(s/defn ^:dynamic *decrypt-byte-string* :- ByteString
  "Dynamic function used to decrypt the individual block vals. Default implementation
  is a no-op; rebind this function to provide actual decryption logic."
  [v :- ByteString] v)
(s/defn encrypt-block :- Block [encrypt :- IFn b :- Block]
  (if (value b) (value b encrypt) b))
(s/defn decrypt-block :- Block [decrypt :- IFn b :- Block]
  (if (value b) (value b decrypt) b))

(s/defschema ReadMode
  "Read Modes control whether the entire Block is loaded from storage, or
  only the Block's attributes."
  (s/enum :read-attrs :read-full))
(s/defschema WriteMode
  "Write Modes control whether the entire Block is written to storage, or
   only the Block's attributes."
  (s/enum :write-attrs :write-full))

(defprotocol BlockStorage
  "Storage is block-addressable: i.e., a block is the smallest unit that Storage is aware of.
   This methods are for Storage implementors and should NEVER be called directly!"
  (storage-read-blocks [s read-mode namespace ids]
    "reads blocks from BlockStore, returns a sequence of blocks.")
  (storage-write-blocks [s write-mode blocks]
    "writes blocks to BlockStore, returns a sequence of maps {:keys [namespace id]} of written blocks")
  (storage-delete-blocks [s namespace ids]
    "deletes blocks from BlockStore, returns a sequence of ids of deleted blocks")
  (storage-compare-and-set-block [s expected-block replacement-block]
    "compare-and-set expected-block for replacement block. Returns true/false to indicate success.")
  (storage-create-block [s block]
    "create-block writes block IFF NO OTHER BLOCK HAS BEEN WRITTEN WITH THAT ID. Returns true/false."))

(def read-blocks-timer
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.read-blocks.timer
                      (em/timer "Times calls to read-blocks and read-blocks-only")))
(def read-blocks-hist
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.read-blocks.histogram
                      (em/histogram (em/reservoir) "Counts blocks requested in each call to read-blocks or read-blocks-only")))

;; BLOCK Storage Operations
;; TODO: Finish deprecating the commented out schema in favor of spec
(d/defn ^{::d/aspects [concurrency-measured]} read-blocks ;; :- [Block]
  "Reads blocks in the given namespace from storage.

  read-mode: specify ':read-full' to read the entire block (attrs + val);
             specify ':read-attrs' to only load the block attrs

  opts keys: specify :decrypt with a function that will be applied to each
             read block. Function will be passed a block and is expected to decrypt
             the block's :val and return the decrypted block.
             If ommited, will default to the dynamic function '*decrypt-block-val*'"
  ([storage ;; :- (s/protocol BlockStorage)
    read-mode ;; :- ReadMode
    namespace ;; :- ID
    ids ;; :- [ID]
    ] (read-blocks storage read-mode namespace ids {}))
  ([storage ;; :- (s/protocol BlockStorage)
    read-mode ;; :- ReadMode
    namespace ;; :- ID
    ids ;; :- [ID]
    {:as opts :keys [decrypt] :or {decrypt *decrypt-byte-string*}} ;; :- {(s/optional-key :decrypt) IFn}
    ] {:pre [(some? namespace) (not-empty ids)]}
   (em/update read-blocks-hist (count ids))
   (em/with-timer read-blocks-timer
     (let [blocks (->> (storage-read-blocks storage read-mode namespace ids)
                       (remove nil?)
                       (map (partial decrypt-block decrypt)))
           ;; need to ensure blocks are returned in requested order
           by-id (reduce (fn [m b] (assoc m (storage-id b) b)) {} blocks)]
       (for [id ids] (by-id id))))))

(s/defn read-blocks-only :- [Block]
  "See read-blocks. This works the same but does NOT decrypt."
  ([storage :- (s/protocol BlockStorage)
    read-mode :- ReadMode
    namespace :- ID
    ids :- [ID]] (read-blocks storage read-mode namespace ids {:decrypt identity})))

(def write-blocks-timer
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.write-blocks.timer
                      (em/timer "Times calls to write-blocks and write-blocks-only")))
(def write-blocks-hist
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.write-blocks.histogram
                      (em/histogram (em/reservoir) "Counts blocks passed in each call to write-blocks or write-blocks-only")))

;; TODO: Finish deprecating the commented out schema in favor of spec
(d/defn ^{::d/aspects [concurrency-measured]} write-blocks ;; :- [{:namespace ID, :id ID}]
  "Writes block into storage. Existing blocks will be overwritten in-place.
  Returns a sequence of the block-ids that were successfully written.

  write-mode: specify ':write-full' to write the entire block (attrs + val)
              specify ':write-attrs to write only the block attrs, leaving the existing val unchanged

  opts keys: specify :encrypt with a function that will be applied to each block before writing.
             Function will be passed a block and is expected to encrypt the blocks's :val and
             return the encrypted block. If ommitted will default to the dynamic function '*encrypt-block-val*'."
  ([storage ;; :- (s/protocol BlockStorage)
    write-mode ;; :- WriteMode
    blocks] ;; :- [Block]
   (write-blocks storage write-mode blocks {}))
  ([storage ;; :- (s/protocol BlockStorage)
    write-mode ;; :- WriteMode
    blocks ;; :- [Block]
    {:keys [encrypt] :or {encrypt *encrypt-byte-string*}}] ;; :- {(s/optional-key :encrypt) IFn}
   {:pre [(not-empty blocks)]}
   (em/update write-blocks-hist (count blocks))
   (em/with-timer write-blocks-timer
     (io! (->> blocks
               (map (partial encrypt-block encrypt))
               (storage-write-blocks storage write-mode))))))

(s/defn write-blocks-only :- [{:namespace ID, :id ID}]
  "See write-blocks. This works the same but does NOT encrypt."
  ([storage :- (s/protocol BlockStorage)
    write-mode :- WriteMode
    blocks :- [Block]] (write-blocks storage write-mode blocks {:encrypt identity})))

;; TODO: Finish deprecating the commented out schema in favor of spec
(d/defn ^{::d/aspects [concurrency-measured]} delete-blocks ;; :- [{:namespace ID, :id ID}]
  "Deletes the specified blocks from storage.
  Returns the ids of successfully deleted blocks.

  Delete is idempotent; deleting and ID that does not exist will return success."
  [storage ;; :- (s/protocol BlockStorage)
   namespace ;; :- ID
   ids] ;; :- [ID]
  {:pre [(some? namespace) (not-empty ids)]}
  (io! (storage-delete-blocks storage namespace ids)))

(def cas-timer
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.cas.timer
                      (em/timer "Times calls to compare-and-set-block")))
(def cas-meter
  (em/get-or-register em/DEFAULT 'eva.v2.storage.core.cas.meter
                      (em/meter "Frequency of calls to compare-and-set-block")))

;; TODO: Finish deprecating the commented out schema in favor of spec
(d/defn ^{::d/aspects [concurrency-measured]} compare-and-set-block ;; :- s/Bool
  "Replaces a block in storage. Operation is atomic. Returns true if swap is successful.

 Compares expected-block to current block in storage. If equal, replacement-block
  overwrites the current block. If not equal, no change occurrs."
  [storage ;; :- (s/protocol BlockStorage)
   expected-block ;; :- Block
   replacement-block] ;; :- Block
  {:pre [(some? expected-block) (some? replacement-block)]}
  (insist (= (storage-namespace expected-block) (storage-namespace replacement-block))
          "Cannot swap blocks in different namespaces.")
  (insist (= (storage-id expected-block) (storage-id replacement-block))
          "Cannot swap blocks with different ids.")
  (em/update cas-meter)
  (em/with-timer cas-timer
    (io! (storage-compare-and-set-block storage expected-block replacement-block))))

(d/defn ^{::d/aspects [concurrency-measured]} create-block ;; :- s/Bool
  "Atomically writes a block to storage IF IT DOES NOT EXIST ALREADY. Returns true if
  the write is successful. If the block already exists, it is NOT overwritten, and
  create-block will return false."
  [storage ;; :- (s/protocol BlockStorage)
   block] ;; :- Block
  {:pre [(some? block)]}
  (io! (storage-create-block storage block)))

;; Single Block operations
(s/defn read-block :- (s/maybe Block)
  [storage :- (s/protocol BlockStorage)
   read-mode :- ReadMode
   namespace :- ID
   id :- ID]
  (first (read-blocks storage read-mode namespace [id])))

(s/defn write-block :- (s/maybe {:namespace ID, :id ID})
  [storage :- (s/protocol BlockStorage)
   write-mode :- WriteMode
   block :- Block]
  (first (write-blocks storage write-mode [block])))

(s/defn delete-block :- (s/maybe {:namespace ID, :id ID})
  [storage :- (s/protocol BlockStorage)
   namespace :- ID
   id :- ID]
  (first (delete-blocks storage namespace [id])))

(defprotocol Loadable
  "Blocks can be partially loaded from storage. This protocol allows testing and loading of
  partially loaded Blocks."
  (loaded? [x] "returns true if item has been loaded")
  (load-from [x block-store] "return copy of item that has been loaded from the block-store"))

(extend-protocol Loadable
  Block
  (loaded? [block] (some? (value block)))
  (load-from [block block-store] (read-block block-store :read-full (storage-namespace block) (storage-id block)))
  Ptr
  (loaded? [ptr] false)
  (load-from [{:keys [namespace id]} block-store] (read-block block-store :read-full namespace id)))
