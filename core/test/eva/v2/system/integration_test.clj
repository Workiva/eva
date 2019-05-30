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

(ns eva.v2.system.integration-test
  (:require [clojure.test :refer :all]
            [eva.api :refer :all]
            [barometer.core :as m]
            [eva.v2.messaging.address :as address]
            [eva.v2.system.peer-connection.core :as peer]
            [eva.v2.system.transactor.core :as transactor]
            [eva.v2.system.indexing.core :as indexing]
            [eva.v2.database.core :as database]
            [eva.v2.messaging.jms.alpha.local-broker :as broker]
            [eva.v2.messaging.node.manager.alpha :as node]
            [eva.v2.messaging.node.local-simulation :as local-msg-2]
            [eva.v2.messaging.node.manager.types :as node-types]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.system.database-catalogue.core :as catalog]
            [eva.v2.system.database-connection.core :as dbc]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.system.protocols :as p]
            [eva.config :as conf]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.local :as h2]
            [eva.v2.storage.local :refer [init-h2-db]]
            [com.stuartsierra.component :as c])
  (:import [java.io File]
           [java.util UUID]
           [java.util.concurrent CountDownLatch]))

(defn base-config
  [database-id storage-config messenger-config]
  (merge {::address/transaction-submission "submit-addr"
          ::address/transaction-publication "pub-addr"
          ::address/index-updates "indexes"
          ::peer/id (java.util.UUID/randomUUID)
          ::transactor/id (UUID/randomUUID)
          ::indexing/id (UUID/randomUUID)
          ::database/id database-id}
         storage-config
         messenger-config))

(defn memory-config
  [database-id]
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id database-id
   ::values/partition-id (java.util.UUID/randomUUID)})

(defn messenger-config
  []
  {:messenger-node-config/type :broker-uri
   :broker-type "org.apache.activemq.ActiveMQConnectionFactory"
   :broker-uri "vm://localhost?broker.persistent=false"})

(defn sql-config
  [database-id]
  {::store-type/storage-type ::store-type/sql
   ::values/partition-id database-id
   ::sql/db-spec (h2/db-spec (h2/temp-file))})

(deftest ensure-autogenetic-release-does-release-everything
  (qp/testing-for-resource-leaks
   (release (connect {:autogenetic true}))))

(deftest peer-reconnect
  (qp/testing-for-resource-leaks
   (let [database-id (UUID/randomUUID)
         config (base-config database-id (memory-config database-id) (messenger-config))
         vs (qu/acquire vs-manager/value-store-manager :random config)
         database-info (catalog/initialize-database* vs database-id)
         transactor (qu/acquire transactor/transactor-manager :random config)
         indexer (qu/acquire indexing/indexer-manager :random config)
         connection (connect config)
         connection-2 (connect config)]
     (try
       (is (= connection connection-2))
       (is @(transact connection []))
       (broker/stop-broker!)
       (broker/broker-uri)
       (release connection)
       (println "Sleeping 1500 ms for transactor to recover")
       (Thread/sleep 1500) ;; Give the transactor its 1000 ms to restart messaging.
       (let [connection (connect config)]
         (is (not= connection connection-2))
         (is @(transact connection [])))
       (finally
         (qu/release transactor true)
         (qu/release indexer true)
         (release connection)
         (release connection-2)
         (broker/stop-broker!)
         (qu/release vs true))))))

(defn- fake-messenger
  [publish-fn]
  (let [status (atom false)
        id (qu/new-resource-id)]
    (reify
      p/PublisherManager
      (open-publisher! [this addr opts] {:connection-error (atom nil)})
      (publish! [this addr pub-data] (publish-fn pub-data))
      (close-publisher! [_ _])
      p/SubscriberManager
      (subscribe! [this id addr f opts] {:connection-error (atom nil)})
      (unsubscribe! [_ _ _])
      p/ResponderManager
      (open-responder! [this addr f opts] {:connection-error (atom nil)})
      (close-responder! [_ _])
      p/ErrorListenerManager
      (register-error-listener [_ _ _ _])
      (unregister-error-listener [_ _])
      qu/SharedResource
      (initiated? [_] @status)
      (status* [_] {})
      (resource-id [_] id)
      (initiate [this] (reset! status true) this)
      (terminate [this] (reset! status false) this)
      (force-terminate [_] (reset! status false)))))

(deftest ensure-multiple-transactors-play-nice
  (qp/testing-for-resource-leaks
   (let [pub-log (atom [])
         publish-fn (fn [tag] #(swap! pub-log conj {:node tag :pub-data %}))]
     (qu/overriding
      [node/messenger-nodes
       {:constructor (fn [_ config]
                       (fake-messenger (publish-fn (::tag config))))
        :discriminator (fn [_ config] [(::tag config) (:broker-uri config)])}
       transactor/transactor-manager
       {:discriminator
        (fn [_ config] [(::tag config) (::database/id config) (::transactor/id config)])}]
      (let [database-id (UUID/randomUUID)
            config (base-config database-id (sql-config database-id) (messenger-config))
            vs (qu/acquire vs-manager/value-store-manager :random config)
            database-info (catalog/initialize-database* vs database-id)
            num-txors 10
            num-txs 20
            configs (map #(assoc config ::tag %) (range num-txors))
            total-txs (* num-txors num-txs)
            start-latch (CountDownLatch. num-txors)
            finish-latch (CountDownLatch. num-txors)
            transactors (for [config configs]
                          (transactor/transactor :testing config))
            futs (doall (map-indexed
                         (fn [i txor]
                           (future
                             (.countDown start-latch)
                             (.await start-latch)
                             (binding [transactor/*max-concurrent-modification-retries* Long/MAX_VALUE]
                               (dotimes [n num-txs]
                                 (Thread/sleep 5)
                                 ;; ^^  sleep to force some interleaving of transactions
                                 ;;     without this, the first transactor to get in 'bullies'
                                 ;;     the rest since it doesn't have to go through the
                                 ;;     slow IO process of updating its state.
                                 (p/process-transaction txor
                                                        {:database-id database-id
                                                         :tx-data [[:db/add (tempid :db.part/user)
                                                                    :db/doc (format "%s-%s" i n)]]})))
                             (.countDown finish-latch)))
                         transactors))]
        (try
          (.await finish-latch)
          (is (apply =
                     total-txs
                     (map (comp eva.api/basis-t p/db-snapshot p/repair-and-reload deref :database-connection)
                          transactors)))
          (let [pub-log @pub-log]
            (is (= total-txs (count pub-log)))
            (is (= (range 1 (inc total-txs))
                   ;; sort since publishes can be disorderly
                   (sort (map (comp :tx-num :pub-data) pub-log))))
            ;; vv did everyone get their txs?
            (is (= (zipmap (range num-txors) (repeat num-txs))
                   (frequencies (map :node pub-log)))))
          ;; is the tx-log in the state we expect it to be?
          (is (= (range (count (->> transactors first :database-connection deref p/log)))
                 (->> transactors first :database-connection deref p/log seq (map (comp :tx-num deref)))))
          (is (= (inc total-txs)
                 (->> transactors first :database-connection deref p/log count)))
          (finally
            (broker/stop-broker!)
            (doseq [config configs]
              (qu/release* transactor/transactor-manager :testing config true))
            (qu/release vs true))))))))

(defn break-publishing [messenger-node]
  (let [messenger-node (atom messenger-node)]
    (reify
      p/PublisherManager
      (open-publisher! [this addr opts] (p/open-publisher! @messenger-node addr opts))
      (publish! [this addr pub-data]
        nil #_(when (< 0.5 (rand))
          (p/publish! @messenger-node addr pub-data)))
      (close-publisher! [_ addr] (p/close-publisher! @messenger-node addr))
      p/SubscriberManager
      (subscribe! [_ id addr f opts] (p/subscribe! @messenger-node id addr f opts))
      (unsubscribe! [_ id addr] (p/unsubscribe! @messenger-node id addr))
      p/ResponderManager
      (open-responder! [_ addr f opts] (p/open-responder! @messenger-node addr f opts))
      (close-responder! [_ addr] (p/close-responder! @messenger-node addr))
      p/RequestorManager
      (open-requestor! [mn addr opts] (p/open-requestor! @messenger-node addr opts))
      (close-requestor! [mn addr] (p/close-requestor! @messenger-node addr))
      (request! [mn addr request-msg] (p/request! @messenger-node addr request-msg))
      p/ErrorListenerManager
      (register-error-listener [mn key f args] (p/register-error-listener @messenger-node key f args))
      (unregister-error-listener [mn key] (p/unregister-error-listener @messenger-node key))

      qu/SharedResource
      (initiated?      [_] (qu/initiated? @messenger-node))
      (status*         [_] (qu/status* @messenger-node))
      (resource-id     [_] (qu/resource-id @messenger-node))
      (initiate        [this] (swap! messenger-node qu/initiate) this)
      (terminate       [this] (swap! messenger-node qu/terminate) this)
      (force-terminate [_] (qu/force-terminate messenger-node)))))

(deftest ensure-peers-can-proceed-without-publishes
  (qp/testing-for-resource-leaks
   (qu/overriding
    [node/messenger-nodes
     {:discriminator (fn [user-id config]
                       [(::tag config) (node-types/messenger-node-discriminator user-id config)])
      :constructor (fn [definition config]
                     (let [real-messenger (node-types/messenger-node-constructor (second definition) config)]
                       (break-publishing real-messenger)))}

     dbc/database-connection-manager
     {:discriminator (fn [user-id config] [user-id (::database/id config) (::tag config)])}

     peer/peer-connection-manager
     {:discriminator (fn [_ config] [(::tag config) (::database/id config)])}]
    (let [database-id (UUID/randomUUID)
          config (base-config database-id (sql-config database-id) (messenger-config))
          vs (qu/acquire vs-manager/value-store-manager :random config)
          database-info (catalog/initialize-database* vs database-id)
          num-conns 10
          txor (qu/acquire transactor/transactor-manager :txor (assoc config ::tag -1))
          conns (doall (for [i (range num-conns)]
                         (connect (assoc config ::tag i))))]
      (try
        ;; prime the pump
        @(transact (nth conns 0) [[:db/add (tempid :db.part/user) :db/ident :test-var]])

        (doall (for [i (range 1 num-conns)]
                 (is (= nil (pull (db (nth conns i)) [:db/ident] :test-var)))))

        (doall (map sync-db (take 5 conns)))
        (doall (map #(.syncDb ^eva.Connection %) (drop 5 conns)))

        (doall (for [i (range 1 num-conns)]
                 (is (= {:db/ident :test-var}
                        (pull (db (nth conns i)) [:db/ident] :test-var)))))
        (finally
          (qu/release vs true)
          (doseq [c conns] (release c))
          (qu/release txor true)
          (broker/stop-broker!)))))))

(deftest ensure-stale-transactors-recover-from-pipeline-failure
  (qp/testing-for-resource-leaks
   (qu/overriding
    [node/messenger-nodes {:constructor (fn [_ _] (fake-messenger (constantly true)))}
     transactor/transactor-manager
     {:discriminator (fn [user-id config] [user-id (::database/id config) (::transactor/id config)])}]
    (let [database-id (UUID/randomUUID)
          config (base-config database-id (sql-config database-id) (messenger-config))
          vs (qu/acquire vs-manager/value-store-manager :random config)
          database-info (catalog/initialize-database* vs database-id)
          num-txors 2
          transactors (for [i (range num-txors)]
                        (qu/acquire transactor/transactor-manager i config))
          staleness-count (->> "eva.v2.system.database-connection.core.staleness-meter"
                               (m/get-metric m/DEFAULT)
                               (m/count))]
      (try (is (= 2 (count (sequence (comp (map deref) (distinct)) transactors))))
           (is (p/process-transaction @(first transactors)
                                      {:database-id database-id
                                       :tx-data [[:db/add 0 :db/doc "foo"]]}))
           (is (p/process-transaction @(second transactors)
                                      {:database-id database-id
                                       :tx-data [[:db.fn/cas 0 :db/doc "foo" "bar"]]}))
           (is (= (inc staleness-count)
                  (->> "eva.v2.system.database-connection.core.staleness-meter"
                       (m/get-metric m/DEFAULT)
                       (m/count))))
           (finally
             (doseq [txor transactors] (qu/release txor true))
             (broker/stop-broker!)
             (qu/release vs true)))))))

(deftest persistent-h2-store
  (let [^File tmpfile (sql/temp-file)
        path (.getPath tmpfile)
        config {:autogenetic              true
                ::database/id             (UUID/randomUUID)
                ::values/partition-id     (UUID/randomUUID)
                ::store-type/storage-type ::store-type/sql
                ::sql/db-spec             (sql/h2-db-spec path)}]
    (qp/testing-for-resource-leaks
     (let [conn (connect config)]
       (try @(transact conn [{:db/id (tempid :db.part/db), :db/ident ::foobar}])
            (finally (release conn)))))

    (qp/testing-for-resource-leaks
     (let [conn (connect config)
           conn2 (connect config)]
       (try
         (is (= 1 (count (datoms (db-snapshot conn) :eavt ::foobar))))
         (is (= conn conn2))
         (finally (release conn)
                  (release conn2)))))))

(deftest distinct-local-connections
  (let [uuid-1 (UUID/randomUUID)
        uuid-2 (UUID/randomUUID)
        config-1a {:autogenetic true
                  ::database/id uuid-1}
        config-1b {:autogenetic true
                  ::database/id uuid-1}
        config-2a {:autogenetic true
                   ::database/id uuid-2}
        config-2b {:autogenetic true
                   ::database/id uuid-2}]
    (qp/testing-for-resource-leaks
     (let [conn-1a (connect config-1a)
           conn-1b (connect config-1b)
           conn-2a (connect config-2a)
           conn-2b (connect config-2b)]
       (try
         (is (= conn-1a conn-1b))
         (is (= conn-2a conn-2b))
         (is (not= conn-1a conn-2a))

         (finally
           (release conn-1a)
           (release conn-1b)
           (release conn-2a)
           (release conn-2b)))))))

(deftest ensure-multiple-everythings-play-nice
  (conf/with-overrides {:eva.database.indexes.max-tx-delta 5}
    (qp/testing-for-resource-leaks
     (let [shared-messenger (local-msg-2/local-messenger)]
       (qu/overriding
        [node/messenger-nodes
         {:discriminator (fn [_ config] (::tag config))
          :constructor
          (fn [_ config] (local-msg-2/facade-messenger shared-messenger (::tag config)))}

         transactor/transactor-manager
         {:discriminator
          (fn [_ config] [(::tag config) (::database/id config) (::transactor/id config)])}

         indexing/indexer-manager
         {:discriminator
          (fn [user-id config] [user-id (::database/id config) (::id config) (::tag config)])}]

        (let [database-id (UUID/randomUUID)
              config (base-config database-id (sql-config database-id) (messenger-config))
              vs (qu/acquire vs-manager/value-store-manager :random config)
              database-info (catalog/initialize-database* vs database-id)
              num-txors 3
              num-idxrs 3
              num-txs 50
              txor-configs (map #(assoc config ::tag %) (range num-txors))
              idxr-configs (map #(assoc config ::tag %) (range num-idxrs))
              total-txs (* num-txors num-txs)
              start-latch (CountDownLatch. num-txors)
              finish-latch (CountDownLatch. num-txors)
              transactors (for [config txor-configs]
                            (transactor/transactor :testing config))
              indexors (doall (for [config idxr-configs]
                                (qu/acquire indexing/indexer-manager :testing config)))
              _ (doall (map deref indexors))
              futs (doall (map-indexed
                           (fn [i txor]
                             (future
                               (.countDown start-latch)
                               (.await start-latch)
                               (binding [transactor/*max-concurrent-modification-retries* Long/MAX_VALUE]
                                 (dotimes [n num-txs]
                                   (Thread/sleep 10 #_(rand-int 20))
                                   ;; ^^  sleep to force some interleaving of transactions
                                   ;;     without this, the first transactor to get in 'bullies'
                                   ;;     the rest since it doesn't have to go through the
                                   ;;     slow IO process of updating its state.
                                   (try (p/process-transaction txor
                                                               {:database-id database-id
                                                                :tx-data [[:db/add 0 :db/doc (format "%s-%s" i n)]]})
                                        (catch Exception e
                                          (clojure.tools.logging/warn e)
                                          (println "failed attempting to add " (format "%s-%s" i n))
                                          (.countDown finish-latch)
                                          (throw e))
                                        )))
                               (.countDown finish-latch)))
                           transactors))]
          (try
            (.await finish-latch)
            (is (apply =
                       total-txs
                       (map (comp eva.api/basis-t p/db-snapshot p/repair-and-reload deref :database-connection)
                            transactors)))

            ;; is the tx-log in the state we expect it to be?
            (is (= (range (count (->> transactors first :database-connection deref p/log)))
                   (->> transactors first :database-connection deref p/log seq (map (comp :tx-num deref)))))
            #_(clojure.pprint/pprint (->> transactors first :database-connection deref p/log seq
                                        (map (comp (juxt count #(remove (fn [d] (= 15 (:a d))) %)) eva.core/entry->datoms deref))))
            (is (= (inc total-txs)
                   (->> transactors first :database-connection deref p/log count)))
            (finally
              (broker/stop-broker!)
              (doseq [config txor-configs]
                (qu/release* transactor/transactor-manager :testing config true))
              (doseq [idxor indexors]
                (qu/release idxor true))
              (qu/release vs true)))))))))
