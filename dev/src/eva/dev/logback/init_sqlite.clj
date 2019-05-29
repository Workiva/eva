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

(ns eva.dev.logback.init-sqlite
  (:require [clojure.java.jdbc :as jdbc]))

(def create-table
  {:logging-event
   "CREATE TABLE IF NOT EXISTS logging_event
   (
     timestmp         BIGINT NOT NULL,
     formatted_message  TEXT NOT NULL,
     logger_name       VARCHAR(254) NOT NULL,
     level_string      VARCHAR(254) NOT NULL,
     thread_name       VARCHAR(254),
     reference_flag    SMALLINT,
     arg0              VARCHAR(254),
     arg1              VARCHAR(254),
     arg2              VARCHAR(254),
     arg3              VARCHAR(254),
     caller_filename   VARCHAR(254) NOT NULL,
     caller_class      VARCHAR(254) NOT NULL,
     caller_method     VARCHAR(254) NOT NULL,
     caller_line       CHAR(4) NOT NULL,
     event_id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
   )"

   :logging-event-property
   "CREATE TABLE IF NOT EXISTS logging_event_property
   (
     event_id BIGINT NOT NULL,
     mapped_key VARCHAR (254) NOT NULL,
     mapped_value TEXT,
     PRIMARY KEY (event_id, mapped_key),
     FOREIGN KEY (event_id) REFERENCES logging_event (event_id)
    )"

   :logging-event-exception
   "CREATE TABLE IF NOT EXISTS logging_event_exception
   (
     event_id BIGINT NOT NULL,
     i SMALLINT NOT NULL,
     trace_line VARCHAR (254) NOT NULL,
     PRIMARY KEY (event_id, i),
     FOREIGN KEY (event_id) REFERENCES logging_event (event_id)
   )"
   })

(def default-file "logs/eva-dev.sqlite")
(defn connection-uri [file] (str "jdbc:sqlite:" file))

(defn -main [& [file]]
  (let [file (or file default-file)]
    (jdbc/with-db-connection [conn (connection-uri file)]
      (jdbc/db-do-commands conn (create-table :logging-event))
      (jdbc/db-do-commands conn (create-table :logging-event-property))
      (jdbc/db-do-commands conn (create-table :logging-event-exception)))))
