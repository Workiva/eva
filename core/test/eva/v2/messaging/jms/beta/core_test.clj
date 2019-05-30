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

(ns eva.v2.messaging.jms.beta.core-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test.alpha :as stest]
            [eva.v2.messaging.jms.beta.connection-factory :refer [new-connection-factory]]
            [eva.v2.messaging.jms.beta.core :as jms])
  (:import (javax.jms Session DeliveryMode Message)
           (java.util.concurrent Phaser)))

(deftest test-coercion-functions
  (testing "delivery-mode"
    (stest/instrument 'jms/delivery-mode-value)
    (are [input result] (= result (jms/delivery-mode-value input))
      DeliveryMode/PERSISTENT DeliveryMode/PERSISTENT
      DeliveryMode/NON_PERSISTENT DeliveryMode/NON_PERSISTENT
      :persistent DeliveryMode/PERSISTENT
      :non-persistent DeliveryMode/NON_PERSISTENT)
    (stest/unstrument 'jms/delivery-mode-value)
    (is (thrown? IllegalArgumentException (jms/delivery-mode-value :foo)))
    (is (thrown? IllegalArgumentException (jms/delivery-mode-value nil))))
  (testing "acknowledge-mode"
    (stest/instrument 'jms/acknowledge-mode-value)
    (are [input result] (= result (jms/acknowledge-mode-value input))
      Session/AUTO_ACKNOWLEDGE Session/AUTO_ACKNOWLEDGE
      Session/CLIENT_ACKNOWLEDGE Session/CLIENT_ACKNOWLEDGE
      Session/DUPS_OK_ACKNOWLEDGE Session/DUPS_OK_ACKNOWLEDGE
      :auto Session/AUTO_ACKNOWLEDGE
      :client Session/CLIENT_ACKNOWLEDGE
      :dups-ok Session/DUPS_OK_ACKNOWLEDGE)
    (stest/unstrument 'jms/acknowledge-mode-value)
    (is (thrown? IllegalArgumentException (jms/acknowledge-mode-value :foo)))
    (is (thrown? IllegalArgumentException (jms/acknowledge-mode-value nil?)))))

(defn test-jms-features [cf]
  (testing (str "integration using connection-factory: " cf)
    (is (jms/connection-factory? cf))
    (try
      (stest/instrument (stest/enumerate-namespace 'eva.v2.messaging.jms.beta.core))
      (testing "connection creation"
        (with-open [c (jms/create-connection cf)]
          (is (jms/connection? c))))
      (testing "session construction and inspection"
        (with-open [c (jms/create-connection cf)
                    s1 (jms/create-session c)
                    s2 (jms/create-session c {:acknowledge-mode :client})
                    s3 (jms/create-session c {:acknowledge-mode :dups-ok})]
          (is (jms/session? s1))
          (is (jms/session? s2))
          (is (jms/session? s3))
          (is (= {:transacted? false :acknowledge-mode :auto}
                 (jms/session-options s1)))
          (is (= {:transacted? false :acknowledge-mode :client}
                 (jms/session-options s2)))
          (is (= {:transacted? false :acknowledge-mode :dups-ok}
                 (jms/session-options s3)))))
      (testing "sending and receiving"
        (let [phaser (Phaser.)
              c2-received-msgs (atom [])
              queue-name "test.queue"]
          (with-open [c (jms/create-connection cf)
                      c1-session (jms/create-session c {:transacted?      false
                                                        :acknowledge-mode :auto})
                      c1-producer (jms/message-producer c1-session (jms/queue c1-session queue-name))

                      c2-session (jms/create-session c {:transacted?      false
                                                        :acknowledge-mode :auto})
                      c2-consumer (jms/message-consumer c2-session
                                                        (jms/queue c2-session queue-name)
                                                        :on-message (do
                                                                      (.register phaser)
                                                                      (fn [^Message msg]
                                                                        (swap! c2-received-msgs conj (jms/message-info msg))
                                                                        (.arrive phaser))))]
            (.register phaser)
            (.start c)
            (let [msg1 (doto (.createTextMessage c1-session "message-1")
                         (jms/set-message-info! {:correlation-id "1"}))
                  msg2 (doto (.createTextMessage c2-session "message-2")
                         (jms/set-message-info! {:correlation-id "2"}))]
              (is (= 0 (count @c2-received-msgs)))
              (jms/send-message! c1-producer msg1)
              (.arriveAndAwaitAdvance phaser)
              (is (= 1 (count @c2-received-msgs)))
              (is (= "1" (:correlation-id (first @c2-received-msgs))))
              (jms/send-message! c1-producer msg2)
              (.arriveAndAwaitAdvance phaser)
              (is (= 2 (count @c2-received-msgs)))
              (is (= "2" (:correlation-id (second @c2-received-msgs))))))))
      (finally
        (stest/unstrument (stest/enumerate-namespace 'eva.v2.messaging.jms.beta.core))))))

(def possible-connection-factories
  #{{:class "org.apache.activemq.ActiveMQConnectionFactory"
     :args ["vm://localhost?broker.persistent=false"]}})

(deftest test:jms-integration
  (let [connection-factories (->> possible-connection-factories
                                  (map (fn [{:keys [class args]}]
                                         (try (apply new-connection-factory class args)
                                              (catch ClassNotFoundException _ nil))))
                                  (remove nil?))]
    (doseq [cf connection-factories]
      (test-jms-features cf))))
