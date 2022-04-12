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

(ns eva.v2.storage.ddb
  (:require [com.stuartsierra.component :as component]
            [eva.v2.storage.core :as block]
            [eva.config :refer [config-strict]]
            [eva.v2.storage.error :refer [raise-ddb-err]]
            [recide.core :refer [try*]]
            [quartermaster.core :as qu]
            [eva.error :refer [insist]]
            [barometer.core :as metrics]
            [clojure.set :as set]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [clojure.edn :as edn]
            [again.core :as again])
  (:import (com.amazonaws.services.dynamodbv2 AmazonDynamoDB AmazonDynamoDBClient)
           (com.amazonaws.auth AWSCredentials BasicAWSCredentials AWSCredentialsProvider DefaultAWSCredentialsProviderChain)
           (com.amazonaws.services.dynamodbv2.model AttributeDefinition
                                                    AttributeValue
                                                    BatchGetItemResult
                                                    BatchWriteItemResult
                                                    ConditionalCheckFailedException
                                                    CreateTableRequest
                                                    DeleteRequest
                                                    KeySchemaElement
                                                    KeyType
                                                    KeysAndAttributes
                                                    ProvisionedThroughput
                                                    PutItemRequest
                                                    PutRequest
                                                    ResourceInUseException
                                                    ReturnValue
                                                    ScalarAttributeType
                                                    WriteRequest)
           (java.util Map)
           (java.io IOException)
           (eva ByteString)
           (eva.error.v1 EvaException)
           (java.nio ByteBuffer)
           (clojure.lang ExceptionInfo)
           (com.amazonaws.services.dynamodbv2.xspec ExpressionSpecBuilder S B UpdateItemExpressionSpec)
           (com.amazonaws.services.dynamodbv2.document Table DynamoDB)
           (com.amazonaws.services.dynamodbv2.document.spec UpdateItemSpec)
           (com.amazonaws.regions Regions Region)))

(defn ->aws-cred [cred]
  (cond (instance? AWSCredentials cred) cred
        (instance? AWSCredentialsProvider cred) cred
        (and (map? cred)
             (contains? cred :access-key-id)
             (contains? cred :secret-key)) (BasicAWSCredentials. (:access-key-id cred) (:secret-key cred))
        :else (raise-ddb-err :credentials
                             "expected map containing: #{:access-key :secret-key}"
                             {::sanex/sanitary? true})))

(defn ->ddb-client ^AmazonDynamoDB [{:as config :keys [cred region endpoint]
                                     :or {cred (DefaultAWSCredentialsProviderChain.)}}]
  (let [ddb-client (AmazonDynamoDBClient. (->aws-cred cred))]
    (when region
      (let [region (if (instance? Region region)
                     region
                     (Region/getRegion (Regions/fromName (str region))))]
        (.setRegion ddb-client region)))
    (when endpoint
      (.setEndpoint ddb-client endpoint))
    ddb-client))

(defn ^CreateTableRequest create-kv-table-request
  [table-name read-capacity write-capacity]
  (doto (CreateTableRequest.)
    (.setTableName table-name)
    (.setKeySchema [(KeySchemaElement. "bid" KeyType/HASH)])
    (.setAttributeDefinitions [(AttributeDefinition. "bid" ScalarAttributeType/S)])
    (.setProvisionedThroughput (ProvisionedThroughput. read-capacity write-capacity))))

(defn create-kv-table [^AmazonDynamoDB client table-name read-capacity write-capacity]
  (let [ddb (DynamoDB. client)
        table (.createTable ddb (create-kv-table-request table-name read-capacity write-capacity))]
    (.waitForActive table)))

(defn ensure-kv-table [^AmazonDynamoDB client table-name read-capacity write-capacity]
  (try (create-kv-table client table-name read-capacity write-capacity)
       (catch ResourceInUseException e nil)))

(defn ^String ddb-block-id
  ([block-or-map]
   (let [[namespace id] (if (satisfies? block/StorageBlock block-or-map)
                          ((juxt block/storage-namespace block/storage-id) block-or-map)
                          ((juxt :namespace :id) block-or-map))]
     (ddb-block-id namespace id)))
  ([namespace id]
   {:pre [(string? namespace) (string? id)]}
   (pr-str [namespace id])))

(defn read-mode-set-attributes! [^KeysAndAttributes ka read-mode]
  (case read-mode
    :read-attrs (.setAttributesToGet ka #{"ns" "id" "attrs"})
    :read-full (.setAttributesToGet ka #{"ns" "id" "attrs" "val"})))

(defn ^KeysAndAttributes batch-keys
  ([read-mode namespace ids]
   (doto (KeysAndAttributes.)
     (.setKeys (for [id ids] {"bid" (AttributeValue. (ddb-block-id namespace id))}))
     (read-mode-set-attributes! read-mode)
     (.setConsistentRead true)))
  ([read-mode blocks]
   (doto (KeysAndAttributes.)
     (.setKeys (for [b blocks] {"bid" (AttributeValue. (ddb-block-id b))}))
     (read-mode-set-attributes! read-mode)
     (.setConsistentRead true))))

(defn ^String dump-attrs [attrs] (pr-str (into (sorted-map) attrs)))
(defn read-attrs [^String s] (edn/read-string s))

(defn item->block
  ([{:strs [ns id attrs val]}]
   (block/->Block (.getS ^AttributeValue ns) (.getS ^AttributeValue id)
                  (read-attrs (.getS ^AttributeValue attrs))
                  (when val (ByteString/copyFrom (.getB ^AttributeValue val))))))

(defn block->item
  ([write-mode blk]
   {:pre [(contains? #{:write-full :write-attrs} write-mode)]}
   (cond->
       {"bid"   (doto (AttributeValue.)
                  (.setS (ddb-block-id (block/storage-namespace blk)
                                       (block/storage-id blk))))
        "ns"    (doto (AttributeValue.)
                  (.setS (block/storage-namespace blk)))
        "id"    (doto (AttributeValue.)
                  (.setS (block/storage-id blk)))
        "attrs" (doto (AttributeValue.)
                  (.setS (dump-attrs (block/attributes blk))))}
     (and (= write-mode :write-full) (block/value blk))
     (assoc "val" (doto (AttributeValue.)
                    (.setB (ByteBuffer/wrap (.toByteArray ^ByteString (block/value blk)))))))))

;; With a max-retries of 20 and a multiplier with base 10 exp 1.25,
;; the following will average retrying over ~3.4s
(def ^:dynamic *ddb-max-retries* (config-strict :eva.v2.storage.ddb.max-retries))
(defn exponential-retry-strategy [retries]
  (cons 0 (again/max-retries retries
            (again/randomize-strategy 0.5
              (again/multiplicative-strategy 10 1.25)))))

(defn retry-until-complete
  ([strategy get-completed get-unprocessed completed-f f init-unprocessed]
   (retry-until-complete strategy nil nil nil get-completed get-unprocessed completed-f f init-unprocessed))
  ([strategy attempts-hist delay-hist unprocessed-items-hist get-completed get-unprocessed completed-f f init-unprocessed]
   (let [rid (random-uuid)
         completed (volatile! (completed-f))
         retries (volatile! 0)]
     (loop [[delay & strategy] strategy
            result (f init-unprocessed)]
       (let [just-completed (get-completed result)
             unprocessed (get-unprocessed result)]
         (vswap! completed completed-f just-completed)
         (if (seq unprocessed)
           (if delay
             (do (log/warnf "DynamoDB request (retry-sequence-id=%s) returned unprocessed items on attempt %s. Completed %s items, %s remain. Backing off for %s ms and retrying."
                            rid
                            @retries
                            (count just-completed)
                            (count unprocessed)
                            delay)
                 (some-> unprocessed-items-hist (metrics/update (count unprocessed)))
                 (some-> delay-hist (metrics/update delay))
                 (Thread/sleep (long delay))
                 (vswap! retries inc)
                 (recur strategy (f unprocessed)))
             (do
               (raise-ddb-err :unprocessed
                              "dynamodb failed to process all requested items."
                              {:unprocessed unprocessed
                               :retries     @retries
                               ::sanex/sanitary? false}))) ;; contains customer data
           (do
             (some-> attempts-hist (metrics/update @retries))
             @completed)))))))

(def ddb-batch-get-attempts
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-get-attempts")
   (metrics/histogram
    (metrics/reservoir)
    "Counts the number of ddb request attempts that occur when trying to fully process all item gets")))

(def ddb-batch-get-attempt-delay-hist
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-get-attempt-delay-ms")
   (metrics/histogram
    (metrics/reservoir)
    "Measures the ms delay between ddb request attempts when try to fully process all item gets")))

(def ddb-batch-get-unprocessed-items-meter
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-get-unprocessed-items")
   (metrics/histogram
    (metrics/reservoir)
    "Measures the number of unprocessed items from a ddb batch get request.")))

(defn ddb-batch-get-item [^AmazonDynamoDB client ^Map table-keys]
  (retry-until-complete
   (exponential-retry-strategy *ddb-max-retries*)
   ddb-batch-get-attempts
   ddb-batch-get-attempt-delay-hist
   ddb-batch-get-unprocessed-items-meter
   #(.getResponses ^BatchGetItemResult %)
   #(.getUnprocessedKeys ^BatchGetItemResult %)
   (fn ([] {})
     ([acc resp] (merge-with concat acc resp)))
   #(.batchGetItem client ^Map %)
   table-keys))

(defn ddb-read-all-blocks
  [^AmazonDynamoDB client table read-mode namespace block-ids]
  (let [bks (batch-keys read-mode namespace block-ids)
        resp (ddb-batch-get-item client {table bks})
        tbl-resp (get resp table)]
    (mapv item->block tbl-resp)))

(defn ^WriteRequest block->write-request [write-mode blk] (WriteRequest. (PutRequest. (block->item write-mode blk))))
(defn ^Map batch-write-blocks-map [table write-mode blocks] {table (mapv #(block->write-request write-mode %) blocks)})

(def ddb-batch-write-retry-counter
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-write-retry-count")
   (metrics/histogram
    (metrics/reservoir)
    "Counts the number of ddb request attempts that occur when trying to fully process all item writes")))

(def ddb-batch-write-attempt-delay-meter
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-write-attempt-delay-ms")
   (metrics/histogram
    (metrics/reservoir)
    "Measures the ms delay between ddb request attempts when try to fully process all item writes")))

(def ddb-batch-write-unprocessed-items-meter
  (metrics/get-or-register
   metrics/DEFAULT
   (str *ns* "." "batch-write-unprocessed-items")
   (metrics/histogram
    (metrics/reservoir)
    "Measures the number of unprocessed items from a ddb batch write request.")))

(defn ddb-batch-write-item [^AmazonDynamoDB client ^Map table-items]
  (retry-until-complete
   (exponential-retry-strategy *ddb-max-retries*)
   ddb-batch-write-retry-counter
   ddb-batch-write-attempt-delay-meter
   ddb-batch-write-unprocessed-items-meter
   (constantly nil)
   #(.getUnprocessedItems ^BatchWriteItemResult %)
   (constantly nil)
   #(.batchWriteItem client ^Map %)
   table-items))

(defn ddb-put-all-blocks
  ([^AmazonDynamoDB client table write-mode blocks]
   (ddb-batch-write-item client (batch-write-blocks-map table write-mode blocks))))

(defn ^WriteRequest block-delete-request [namespace id]
  (WriteRequest. (DeleteRequest. {"bid" (AttributeValue. (ddb-block-id namespace id))})))
(defn ^Map batch-delete-blocks-map [table namespace ids]
  {table (mapv #(block-delete-request namespace %) ids)})

(defn ddb-delete-all-blocks
  [^AmazonDynamoDB client table namespace ids]
  (ddb-batch-write-item client (batch-delete-blocks-map table namespace ids)))

(defn ^S spec-S [name] (ExpressionSpecBuilder/S name))
(defn ^B spec-B [name] (ExpressionSpecBuilder/B name))

(defn ^UpdateItemExpressionSpec compare-set-block-spec
  [expected-block replacement-block]
  (let [expected-ns (block/storage-namespace expected-block)
        replacement-ns (block/storage-namespace replacement-block)
        _ (insist (= expected-ns replacement-ns)
                  "Cannot swap blocks in different namespaces.")
        expected-id (block/storage-id expected-block)
        replacement-id (block/storage-id replacement-block)
        _ (insist (= expected-id replacement-id)
                  "Cannot swap blocks with different ids")

        expected-attrs (dump-attrs (block/attributes expected-block))
        ^bytes expected-val (.toByteArray (block/value expected-block))

        replacement-attrs (dump-attrs (block/attributes replacement-block))
        ^bytes replacement-val (.toByteArray (block/value replacement-block))]

    (-> (ExpressionSpecBuilder.)
        (.addUpdate (-> (spec-S "attrs") (.set replacement-attrs)))
        (.addUpdate (-> (spec-B "val") (.set replacement-val)))
        (.withCondition (-> (spec-S "attrs") (.eq expected-attrs)
                            (.and
                             (-> (spec-B "val") (.eq expected-val)))))
        (.buildForUpdate))))

(defn ddb-compare-and-set-block
  [^AmazonDynamoDB client table-name expected-block replacement-block]
  (let [table (Table. client table-name)
        expected-item (block->item :write-full expected-block)
        replacement-item (block->item :write-full replacement-block)
        outcome (.updateItem table
                             (-> (UpdateItemSpec.)
                                 (.withReturnValues ReturnValue/ALL_NEW)
                                 (.withPrimaryKey "bid" (ddb-block-id expected-block))
                                 (.withExpressionSpec (compare-set-block-spec expected-block replacement-block))))]

    outcome))

(defn ddb-create-block-put-item-request
  [table-name block]
  (-> (PutItemRequest.)
      (.withTableName ^String table-name)
      (.withItem ^Map (block->item :write-full block))
      (.withConditionExpression "attribute_not_exists(bid)")))

(defn ddb-create-block
  [^AmazonDynamoDB client table-name block]
  (let [item (block->item :write-full block)
        request (ddb-create-block-put-item-request table-name block)]
    (.putItem client request)))

(defrecord DDBStorage [config ^AmazonDynamoDB client table]
  qu/SharedResource
  (resource-id [this] (some-> this ::resource-id deref))
  (initiate [{:as this :keys [create-table?]}]
    (insist (or (some? client) (some? config)))
    (insist (some? table))
    (if (qu/initiated? this)
      this
      (let [_ (log/debug "Starting DDBStorage using table: " table)
            client (->ddb-client config)]
        (when create-table?
          (log/debug "ensuring dynamodb table exists: " table)
          (ensure-kv-table client table 5 5))
        (assoc this
               :client client
               ::resource-id (atom (qu/new-resource-id))))))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (terminate [this]
    (if-not (qu/initiated? this)
      this
      (do (.shutdown client)
          (reset! (::resource-id this) nil)
          (assoc this :client nil))))
  (force-terminate [this] (qu/terminate this))
  component/Lifecycle
  (start [this] (qu/initiate this))
  (stop [this] (qu/terminate this))
  block/BlockStorage
  (storage-read-blocks [this read-mode namespace ids]
    (qu/ensure-initiated! this "cannot read blocks.")
    (ddb-read-all-blocks client table read-mode namespace ids))
  (storage-write-blocks [this write-mode blocks]
    (qu/ensure-initiated! this "cannot write blocks.")
    (try*
     (ddb-put-all-blocks client table write-mode blocks)
     (for [b blocks] {:namespace (block/storage-namespace b)
                      :id (block/storage-id b)})
     (catch EvaException e
       (let [{:keys [:eva/error unprocessed]} (ex-data e)]
         (if (not= error :storage.dynamo/unprocessed)
           (throw e)
           (let [all-block-ids (for [blk blocks]
                                 {:namespace (block/storage-namespace blk)
                                  :id (block/storage-id blk)})
                 unprocessed-block-ids (for [^WriteRequest write-req (get unprocessed table)
                                             :let [^PutRequest put-item (.getPutRequest write-req)
                                                   item (.getItem put-item)
                                                   blk (item->block item)]]
                                         {:namespace (block/storage-namespace blk)
                                          :id (block/storage-id blk)})]
             (seq (set/difference (set all-block-ids) (set unprocessed-block-ids)))))))))
  (storage-delete-blocks [this namespace ids]
    (qu/ensure-initiated! this "cannot delete blocks.")
    (try
      (ddb-delete-all-blocks client table namespace ids)
      (for [id ids] {:namespace namespace :id id})
      (catch EvaException e
        (let [{:keys [:eva/error unprocessed]} (ex-data e)]
          (if (not= error :storage.dynamo/unprocessed)
            (throw e)
            (let [bid->block-ids (into {}
                                       (for [id ids]
                                         [(ddb-block-id namespace id)
                                          {:namespace namespace :id id}]))
                  unprocessed-bids (set (for [^WriteRequest write-req (get unprocessed table)
                                              :let [^DeleteRequest del-req (.getDeleteRequest write-req)
                                                    item-key (.getKey del-req)
                                                    bid (.getS ^AttributeValue (get item-key "bid"))]]
                                          bid))]
              (for [[bid block-id] bid->block-ids
                    :when (not (unprocessed-bids bid))]
                block-id)))))))
  (storage-compare-and-set-block [this expected-block replacement-block]
    (qu/ensure-initiated! this "cannot cas block.")
    (try
      (ddb-compare-and-set-block client table expected-block replacement-block)
      true
      (catch ConditionalCheckFailedException e
        false)))
  (storage-create-block [this block]
    (qu/ensure-initiated! this "cannot create block.")
    (try
      (boolean (ddb-create-block client table block))
      (catch ConditionalCheckFailedException e
        false))))
