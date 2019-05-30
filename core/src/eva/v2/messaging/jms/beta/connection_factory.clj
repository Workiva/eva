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

(ns eva.v2.messaging.jms.beta.connection-factory
  (:import (java.lang.reflect Constructor)
           (javax.jms ConnectionFactory)))

(defn ^:private load-class [cls]
  (cond (class? cls) cls
        (string? cls) (Class/forName (str cls))
        (symbol? cls) (recur (name cls))
        :else (throw (IllegalArgumentException. "expected class, or class-name as string or symbol"))))

(defn ^:private constructor [cls sig] (.getConstructor (load-class cls) (into-array Class sig)))
(defn ^:private new-instance [^Constructor ctor args]
  (.newInstance ctor (into-array Object args)))

(defn ^:private construct
  ([cls args] (construct cls (map class args) args))
  ([cls sig args] (-> cls (constructor sig) (new-instance args))))

(defn ^ConnectionFactory new-connection-factory
  "Creates an instance of a javax.jms.ConnectionFactory
  given the class-name and the constructor arguments."
  [class-name & args]
  (let [cls (load-class class-name)]
    (if (isa? cls ConnectionFactory)
      (construct cls args)
      (throw (IllegalArgumentException. (str cls " does not implement " ConnectionFactory))))))

(def connection-factory-name
  "Provides a map of alias to ConnectionFactory class-name.
  May be used in conjunction with `new-connection-factory` for easier
  lookup of class-names."
  {:artemis "org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory"
   :activemq "org.apache.activemq.ActiveMQConnectionFactory"})
