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

(ns eva.dev.logback.print-appender
  (:require [clojure.string :as str]
            [clojure.pprint :refer [pprint]])
  (:import (java.util Date)
           (ch.qos.logback.classic.spi ILoggingEvent)))

(defn event->form [^ILoggingEvent evt]
  (tagged-literal (symbol "log" (str/lower-case (str (.getLevel evt))))
                  [(Date. (.getTimeStamp evt))
                   (tagged-literal 'thread
                                   (.getThreadName evt))
                   (tagged-literal 'logger
                                   (.getLoggerName evt))
                   (tagged-literal 'msg (.getFormattedMessage evt))]))

(defn prn-event [^ILoggingEvent evt] (prn (event->form evt)))
(defn pprint-event-map [^ILoggingEvent evt]
  (pprint (array-map :level (symbol (str/lower-case (str (.getLevel evt))))
                     :logger (symbol (.getLoggerName evt))
                     :time (Date. (.getTimeStamp evt))
                     :thread (symbol (.getThreadName evt))
                     :msg (.getFormattedMessage evt))))

(defn simple-print-event [^ILoggingEvent evt]
  (println (format "[%s/%s] <%s> %s"
                   (str (.getLevel evt))
                   (.getLoggerName evt)
                   (.getThreadName evt)
                   (.getFormattedMessage evt))))

(def print-logging-event simple-print-event)

(defn simple-printing! [] (alter-var-root #'print-logging-event (constantly simple-print-event)))
(defn map-printing! [] (alter-var-root #'print-logging-event (constantly pprint-event-map)))
(defn edn-printing! [] (alter-var-root #'print-logging-event (constantly prn-event)))
