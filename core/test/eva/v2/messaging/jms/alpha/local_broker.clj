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

(ns eva.v2.messaging.jms.alpha.local-broker
  (:require [quartermaster.core :as qu])
  (:import (java.util ArrayList)
           (org.apache.activemq.artemis.jms.server.embedded EmbeddedJMS)
           (org.apache.activemq.artemis.jms.server.config JMSConfiguration)
           (org.apache.activemq.artemis.core.config Configuration)
           (org.apache.activemq.artemis.api.core TransportConfiguration)
           (org.apache.activemq.artemis.core.remoting.impl.netty NettyAcceptorFactory NettyConnectorFactory)
           (org.apache.activemq.artemis.core.remoting.impl.invm InVMConnectorFactory InVMAcceptorFactory)
           (org.apache.activemq.artemis.core.config.impl ConfigurationImpl)
           (org.apache.activemq.artemis.jms.server.config.impl ConnectionFactoryConfigurationImpl JMSConfigurationImpl)))

;; Embedded Server

(defn ^TransportConfiguration in-vm-acceptor-config [] (TransportConfiguration. (.getName InVMAcceptorFactory)))
(defn ^TransportConfiguration in-vm-connector-config [] (TransportConfiguration. (.getName InVMConnectorFactory)))

(defn ^TransportConfiguration netty-acceptor-config [] (TransportConfiguration. (.getName NettyAcceptorFactory)))
(defn ^TransportConfiguration netty-connector-config [] (TransportConfiguration. (.getName NettyConnectorFactory)))

(defn ^Configuration simple-core-configuration
  [connector-id
   ^TransportConfiguration connector-config
   ^TransportConfiguration acceptor-config]
  (doto (ConfigurationImpl.)
    (.setPersistenceEnabled false)
    (.setSecurityEnabled false)
    (.addConnectorConfiguration (str connector-id) connector-config)
    (.addAcceptorConfiguration acceptor-config)))

(defn ^JMSConfiguration simple-jms-configuration [connection-factory-id connector-id]
  (let [conn-factory-config (doto (ConnectionFactoryConfigurationImpl.)
                              (.setName (str connection-factory-id))
                              (.setConnectorNames (doto (ArrayList.)
                                                    (.add (str connector-id))))
                              (.setBindings (into-array String [(str connection-factory-id)])))
        jms-config (JMSConfigurationImpl.)]
    (-> jms-config
        (.getConnectionFactoryConfigurations)
        (.add conn-factory-config))
    jms-config))

(defn ^EmbeddedJMS jms-server [^Configuration core-config
                               ^JMSConfiguration jms-config]
  (-> (EmbeddedJMS.)
      (.setConfiguration core-config)
      (.setJmsConfiguration jms-config)))

;; Local Brokers

(qu/defmanager brokers
  :discriminator (constantly :eva-system) ;; it's global
  :constructor (fn [_ _]
                 (.start
                  (jms-server (simple-core-configuration "connector"
                                                         (netty-connector-config)
                                                         (netty-acceptor-config))
                              (simple-jms-configuration "cf" "connector"))))
  :terminator (fn [item] (.stop ^EmbeddedJMS item)))

(defn broker-uri [] (.. @(qu/acquire brokers :blah :blah) (lookup "cf") toURI))
(defn stop-broker! [] (qu/release* brokers :blah :blah true))
