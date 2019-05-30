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

(ns eva.load-tests
  (:require [eva.api :as eva :refer [connect squuid transact db]]
            [clojure.test :refer :all]
            [eva.datastructures.utils.comparators :refer [UPPER LOWER]]
            [com.rpl.specter.macros :refer [transform]]
            [clojure.tools.logging :refer [debug]]
            [eva.logging :refer [with-context]]
            [eva.v2.server.transactor-test-utils
             :refer [with-local-sql-connection with-local-mem-connection]]))

(def tx-times (atom []))

(defn ->ms [x]
  (float (/ x 1e6)))

(defn local-db-path
  [filename]
  (str (System/getProperty "user.dir") "/" filename))

(defn n->tx [ns]
  (doall (for [n ns]
           [:db/add (eva/tempid :db.part/user) :numbah n])))

(defn rtransact [conn tx]
  (let [start (. java.lang.System (clojure.core/nanoTime))
        _ @(eva/transact conn tx)
        duration (- (. java.lang.System (clojure.core/nanoTime)) start)]
    (swap! tx-times conj duration)
    conn))

(def tx-count (volatile! 0))

(deftest unit:load-the-integers
  (with-local-sql-connection conn
    (eva.config/with-overrides {:eva.transact-timeout 1e6}
      (let [numbahs-schema [{:db/id                 (eva/tempid :db.part/db)
                             :db.install/_attribute :db.part/db
                             :db/ident              :numbah
                             :db/doc                "ITS A NUMBAH"
                             :db/valueType          :db.type/long
                             :db/cardinality        :db.cardinality/one}]
            _ @(eva/transact conn numbahs-schema)
            txs-count 100
            tx-size 1000
            xf (comp
                (partition-all tx-size)
                (take txs-count)
                (map n->tx))
            transactions (into [] xf (range))]
        (vreset! tx-count 0)
        (doseq [tx transactions]
          (let [tx-num (vswap! tx-count inc)]
            (when (= 0 (mod tx-num 100)) (debug "tx:" tx-num)))
          (rtransact conn tx))))))

(def graph-schema [{:db/id                 (eva/tempid :db.part/db)
                    :db.install/_attribute :db.part/db
                    :db/ident              :children
                    :db/valueType          :db.type/ref
                    :db/isComponent        true
                    :db/cardinality        :db.cardinality/many}
                   {:db/id                 (eva/tempid :db.part/db)
                    :db.install/_attribute :db.part/db
                    :db/ident              :label
                    :db/valueType          :db.type/long
                    :db/unique             :db.unique/identity
                    :db/cardinality        :db.cardinality/one}])

(defn create-random-forest
  "Given a collection of maps, create a random forest of trees by adding a
   :children key-value pair to each map, with maximum
   branching factor max-branch (all nodes will have between 1 and max-branch
   children).  If provided, apply the function node-f to each node as they
   are added to the tree."
  ([max-branch maps] (create-random-forest identity max-branch maps))
  ([node-f max-branch maps]
   (assert (>= max-branch 2))
   (let [cur-branch (inc (rand-int max-branch))]
     (if (>= cur-branch (count maps))
       (mapv node-f maps)
       (let [[cur-nodes descendants]
             (split-at cur-branch maps)]
         (->> (group-by (fn [_] (rand-nth cur-nodes)) descendants) ;; quick / dirty uniform grouping
              (mapv (fn [[m vs]] (node-f (assoc m :children (create-random-forest node-f max-branch vs)))))))))))

(deftest unit:create-tree
  (with-local-sql-connection conn
    (eva.config/with-overrides {:eva.transact-timeout             1e6
                                :eva.database.overlay.max-size-mb 128}
      (let [_ @(eva/transact conn graph-schema)
            max-branch 5
            transactions 100
            nodes-per-tx 100
            node-colls (->> (for [n (range (* nodes-per-tx transactions))]
                              {:label n})
                            (partition nodes-per-tx))
            trees (map-indexed (fn [i nodes]
                                 {:label    (- i)
                                  :db/id    (eva/tempid :db.part/user)
                                  :children (create-random-forest #(assoc % :db/id (eva/tempid :db.part/user))
                                                                  3
                                                                  nodes)}) node-colls)]
        (doall (map-indexed
                (fn [i tree]
                  @(eva/transact conn [tree])
                  (eva/pull (eva/db conn) '[*] [:label (- i)]))
                trees))))))
