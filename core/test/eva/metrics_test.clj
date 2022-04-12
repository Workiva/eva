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

(ns eva.metrics-test
  (:require [barometer.core :as m]
            [eva.api]
            [eva.v2.database.core :as database]
            [eva.contextual.tags :as ct]
            [eva.contextual.config :as config]
            [eva.contextual.metrics :as metrics]
            [clojure.string :as str]
            [clojure.test :refer :all]))

(def metrics {"eva.api.as-of.timer"        :tagged-with-db-configurable
              "eva.api.connect*.timer"     :tagged-with-db-configurable
              "eva.api.connection-status." :never-db-specific
              "eva.api.datoms.timer"       :tagged-with-db-configurable
              "eva.api.db-snapshot.timer"  :tagged-with-db-configurable
              "eva.api.db.timer"           :never-db-specific
              "eva.api.entid.timer"        :tagged-with-db-configurable
              "eva.api.entid-strict.timer" :tagged-with-db-configurable
              "eva.api.history.timer"      :tagged-with-db-configurable
              "eva.api.pull.timer"         :tagged-with-db-configurable
              "eva.api.pull-many.timer"    :tagged-with-db-configurable
              "eva.api.q."                 :never-db-specific
              "eva.api.release.timer"      :tagged-with-db-configurable
              "eva.api.sync-db.timer"      :tagged-with-db-configurable
              "eva.api.touch.timer"        :tagged-with-db-configurable
              "eva.api.transact.timer"     :tagged-with-db-configurable
              "eva.api.tx-range.timer"     :tagged-with-db-configurable
              "eva.v2.system.transactor.core.process-transaction-impl.timer" :tagged-with-db-always})

(defn- prefixes-of
  ([metric-names s]
   (->> metric-names
        (filter #(str/starts-with? s %1))
        first))
  ([s]
   (prefixes-of (keys metrics) s)))

(defn- clean-metrics []
  (m/remove-matching m/DEFAULT (m/metric-filter (fn [name _] (-> name prefixes-of not-empty)))))

(defn- query-all-idents [db]
  (eva.api/q '[:find ?v :where [_ :db/ident ?v]] db))

(defmacro use-eva [database-id tags-config & body]
  `(do (clean-metrics)
       (config/reset!)
       (dorun (for [[t# v#] ~tags-config] (config/set-tag! t# v#)))
       ;; below are just some db activity in order to generate some logging
       (let [connection#    (eva.api/connect {:local true ::database/id (random-uuid)})
             ~database-id   (get-in connection# [:config ::database/id])
             db#            (eva.api/db connection#)
             db#            (eva.api/db-snapshot connection#)
             txact-results# @(eva.api/transact connection# [[:db/add (eva.api/tempid :db.part/user) :db/doc "doc"]])
             datoms#        (eva.api/datoms db# :aevt)
             entid#         (eva.api/entid db# :db/ident)
             entid#         (eva.api/entid-strict db# :db/ident)
             entity#        (eva.api/entity db# entid#)
             entities#      (eva.api/pull db# [:db/ident] entid#)
             entities#      (eva.api/pull-many db# [:db/ident] [entid#])
             db#            (eva.api/as-of db# (eva.api/basis-t db#))
             db#            (eva.api/sync-db connection#)
             log#           (eva.api/log connection#)
             idents#        (query-all-idents db#)]
         (eva.api/touch entity#)
         (eva.api/history db#)
         (eva.api/tx-range log# 0 1)
         (eva.api/release connection#)
         ~@body
         (metrics/release-metrics))))

(deftest unit:tagged-with-db-configurable
  (let [per-db-configurable (->> metrics
                                 (filter #(= (val %) :tagged-with-db-configurable))
                                 (map first))]

    (use-eva
     database-id
     {::ct/database-id true}
     (dorun
      (for [m per-db-configurable]
        (let [metric-name (str m "?database-id=" database-id)]
          (is (some? (m/get-metric m/DEFAULT metric-name)) (format "%s is missing" metric-name))))))

    (use-eva
     database-id
     {::ct/database-id false}
     (dorun
      (for [m per-db-configurable]
        (is (some? (m/get-metric m/DEFAULT m)) (format "%s is missing" m)))))))

(deftest unit:tagged-with-db-always
  (let [per-db-configurable (->> metrics
                                 (filter #(= (val %) :tagged-with-db-always))
                                 (map first))]

    (use-eva
     database-id
     {::ct/database-id true}
     (dorun
      (for [m per-db-configurable]
        (let [metric-name (str m "?database-id=" database-id)]
          (is (some? (m/get-metric m/DEFAULT metric-name)) (format "%s is missing" metric-name))))))

    (use-eva
     database-id
     {::ct/database-id true}
     (dorun
      (for [m per-db-configurable]
        (let [metric-name (str m "?database-id=" database-id)]
          (is (some? (m/get-metric m/DEFAULT metric-name)) (format "%s is missing" metric-name))))))))

(deftest unit:never-db-specific
  (let [never-db-specific (->> metrics
                               (filter #(= (val %) :never-db-specific))
                               (map first))]

    (is (not-empty never-db-specific))
    (use-eva
     database-id
     {::ct/database-id true}
     (let [metric-names (m/names m/DEFAULT)]
       (dorun
        (for [m metric-names]
          (is (some? (m/get-metric m/DEFAULT m)) (format "%s is missing" m))))))))
