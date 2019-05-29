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

(ns eva.transaction-pipeline.lookup-refs-test
  (:require [eva.api :as eva]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [eva.v2.database.lookup-refs :as lookup-refs]
            [eva.error :as err]
            [clojure.test :refer :all]
            [bond.james :as bond])
  (:import (java.util.concurrent ExecutionException)))

(defn- lookup-ref-ctor [n] [:db/add [:uniq-attr (str n)] :non-uniq-attr (str n)])
(defn- uniq-value-ctor [n] [:db/add (eva/tempid :db.part/user) :uniq-attr (str n)])

(defmacro with-uniq-attrs [total-name total-value conn-name & body]
  `(with-local-mem-connection ~conn-name
     (let [~total-name  ~total-value
           schema#      [{:db.install/_attribute :db.part/db
                          :db/id                 (eva/tempid :db.part/db)
                          :db/ident              :uniq-attr
                          :db/valueType          :db.type/string
                          :db/unique             :db.unique/identity
                          :db/cardinality        :db.cardinality/one}
                         {:db.install/_attribute :db.part/db
                          :db/id                 (eva/tempid :db.part/db)
                          :db/ident              :non-uniq-attr
                          :db/valueType          :db.type/string
                          :db/cardinality        :db.cardinality/one}]
           _#           @(eva/transact ~conn-name schema#)
           uniq-values# (vec (map uniq-value-ctor (range ~total-value)))
           _#           @(eva/transact ~conn-name uniq-values#)]
       ~@body)))


(deftest unit:resolves-lookup-refs-in-batches
  (with-uniq-attrs total 100 conn
    (let [lookup-ref-values (vec (map lookup-ref-ctor (range total)))]
      (bond/with-spy [lookup-refs/resolve-lookup-ref
                      lookup-refs/resolve-lookup-ref-strict
                      lookup-refs/batch-resolve-lookup-refs]
        @(eva/transact conn lookup-ref-values)
        (is (= 0 (-> lookup-refs/resolve-lookup-ref bond/calls count)))
        (is (= 0 (-> lookup-refs/resolve-lookup-ref-strict bond/calls count)))
        (is (> (-> lookup-refs/batch-resolve-lookup-refs bond/calls count) 0))))))

(deftest unit:resolves-all-lookup-refs-correctly
  (with-uniq-attrs total 100 conn
    (let [lookup-ref-values (vec (map lookup-ref-ctor (range total)))
          _                 @(eva/transact conn lookup-ref-values)
          datoms            (eva/q '[:find ?e ?v1 ?v2 :in $ :where [?e :uniq-attr ?v1] [?e :non-uniq-attr ?v2]] (eva/db conn))]
      (is (= total (count datoms))))))

(deftest unit:throws-when-some-lookup-refs-cannot-be-found
  (with-uniq-attrs total 1 conn
    (let [lookup-ref-values (vec (map lookup-ref-ctor (range 2)))
          unwrapper         (fn [e]
                              (is (instance? ExecutionException e))
                              (.getCause ^Exception e))]
      (err/is-thrown?
       {:unwrapper unwrapper}
       @(eva/transact conn lookup-ref-values)))))

(deftest unit:resolves-half-of-lookup-refs-correctly
  (with-uniq-attrs total 100 conn
    (let [half-of-total (int (/ total 2))
          lookup-ref-values (vec (map lookup-ref-ctor (range half-of-total)))
          _                 @(eva/transact conn lookup-ref-values)
          datoms            (eva/q '[:find ?e ?v1 ?v2 :in $ :where [?e :uniq-attr ?v1] [?e :non-uniq-attr ?v2]] (eva/db conn))]
      (is (= half-of-total (count datoms))))))
