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

(ns eva.v2.server.transactor-test-utils
  (:require [eva.api :as api]
            [eva.quartermaster-patches :as qp])
  (:import (java.util UUID)
           (java.io File)))

(defmacro with-local-mem-connection [name & body]
  `(qp/testing-for-resource-leaks
    (let [~name (api/connect {:local true
                              :eva.v2.database.core/id (UUID/randomUUID)})]
      (try
        ~@body
        (finally (api/release ~name))))))

(def cntr (atom 1))
(defn ^File temp-file
  []
  (File/createTempFile (str "sql-test-db-" (swap! cntr inc)) "tmpdb"))

(defn h2-config [path]
  {:local true,
   :eva.v2.database.core/id (UUID/randomUUID),
   :eva.v2.storage.value-store.core/partition-id (UUID/randomUUID),
   :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
   :eva.v2.storage.block-store.impl.sql/db-spec
   {:classname "org.h2.Driver",
    :subprotocol "h2",
    :subname path
    :user "sa"}})

(defn sqlite-config [path]
  {:local true,
   :eva.v2.database.core/id (UUID/randomUUID),
   :eva.v2.storage.value-store.core/partition-id (UUID/randomUUID),
   :eva.v2.storage.block-store.types/storage-type :eva.v2.storage.block-store.types/sql,
   :eva.v2.storage.block-store.impl.sql/db-spec
   {:classname "org.sqlite.JDBC",
    :subprotocol "sqlite",
    :subname path}})

(defmacro with-local-sql-connection* [name & body]
  `(let [tmp-file# (temp-file)
         config# (sqlite-config (.getAbsolutePath tmp-file#))
         ~name (api/connect config#)]
     (try
       ~@body
       (finally (api/release ~name)))))

(defmacro with-local-sql-connection [name & body]
  `(qp/testing-for-resource-leaks
     (with-local-sql-connection* ~name ~@body)))
