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

(ns eva.v2.messaging.jms.activemq-and-artemis-interop
  (:require [clojure.test :refer :all])
  (:import (java.io FileNotFoundException)
           (javax.jms Connection Session MessageListener TextMessage)))

(defn ^:private ns-requirable? [ns-sym]
  (try (require ns-sym)
       (catch FileNotFoundException e nil))
  (find-ns ns-sym))

(defn ^:private class-exists? [class-sym]
  (try (Class/forName (name class-sym))
       (catch ClassNotFoundException _ false)))

(defmacro ^:private if-evals
  ([pred-form success-form]
   `(if-evals ~pred-form ~success-form ~nil))
  ([pred-form success-form failure-form]
   (if (eval pred-form)
     `~success-form
     `~failure-form)))

(defmacro ^:private when-evals [pred-form & success-form]
  `(if-evals ~pred-form (do ~@success-form)))


(when-evals (and (ns-requirable? 'eva.v2.messaging.local-broker)
                 (class-exists? 'org.apache.activemq.ActiveMQConnectionFactory))
  (deftest test-activemq-client-with-artemis-server
    (try
      (let [^java.net.URI artemis-uri (eva.v2.messaging.local-broker/broker-uri)
            _ (println "artemis-uri = " artemis-uri)
            activemq-uri (java.net.URI. (.getScheme artemis-uri)
                                        nil
                                        (.getHost artemis-uri)
                                        (.getPort artemis-uri)
                                        nil
                                        nil
                                        nil)
            _ (println "activemq-uri = " activemq-uri)
            cf (org.apache.activemq.ActiveMQConnectionFactory. activemq-uri)]
        (with-open [^Connection conn (.createConnection cf)]
          (let [s1 (.createSession conn false Session/AUTO_ACKNOWLEDGE)
                s2 (.createSession conn false Session/AUTO_ACKNOWLEDGE)
                q (.createQueue s1 "test-q")
                producer (.createProducer s1 q)
                consumer (.createConsumer s2 q)
                received (atom [])
                message-listener (reify MessageListener
                                   (onMessage [_ msg]
                                     (try (let [txt (.getText ^TextMessage msg)]
                                            (swap! received conj txt))
                                          (catch Throwable e
                                            (println "onMessage error: " e)
                                            (throw e)))))]
            (.setMessageListener consumer message-listener)
            (.start conn)
            (is (= [] @received))
            (.send producer q (.createTextMessage s1 "test 1"))
            (Thread/sleep 100)
            (is (= ["test 1"] @received))
            (.send producer q (.createTextMessage s1 "test 2"))
            (Thread/sleep 100)
            (is (= ["test 1" "test 2"] @received))
            (.send producer q (.createTextMessage s1 "test 3"))
            (Thread/sleep 100)
            (is (= ["test 1" "test 2" "test 3"] @received)))))
      (finally
        (eva.v2.messaging.local-broker/stop-broker!)))))
