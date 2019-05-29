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

(ns eva.functions-test
  (:require [eva.functions :refer :all]
            [schema.test]
            [clojure.test :refer :all]
            [clojure.edn]
            [clojure.string :as cstr]
            [eva.readers]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [eva.api :as api]))

(def api-fn
  (clojure.edn/read-string {:readers *data-readers*}
                           '"#db/fn {:lang \"clojure\"
                                      :params []
                                      :code (d/squuid)}"))

(def require-fn
  (clojure.edn/read-string {:readers *data-readers*}
                           '"#db/fn {:lang \"clojure\"
                                     :params [s1 s2]
                                     :requires [[clojure.set :as s]]
                                     :code (s/union s1 s2)}"))

(def import-fn
  (clojure.edn/read-string {:readers *data-readers*}
                           '"#db/fn {:lang \"clojure\"
                                     :params []
                                     :imports [[java.util UUID]]
                                     :code (UUID. 0 0)}"))

(deftest unit:import-fn
  (is (instance? java.util.UUID (import-fn))))

(deftest unit:require-fn
  (is (= #{:a :b :c}
         (require-fn #{:a :b} #{:b :c}))))

(deftest unit:namespace-require
  ;; squuid isn't in scope
  (is (thrown? Exception (eval '(squuid))))
  ;; now it is!
  (is (instance? java.util.UUID (api-fn))))

(deftest unit:function-with-let-body
  (let [f (compile-db-fn {:lang   :clojure
                          :params '[x]
                          :code   '(let [y (inc x)]
                                     {:x x :y y})})]
    (is (= (f 1) {:x 1 :y 2}))))

(deftest unit:named-functions
  (testing "local compilation"
    (let [f1 (compile-db-fn {:lang    :clojure
                             :fn-name 'foobarbaz
                             :params  '[x]
                             :code    '(throw (Exception. "foo!"))})
          f2 (compile-db-fn {:lang   :clojure
                             :params '[x]
                             :code   '(throw (Exception. "baloney!"))})
          ex-str1 (try (f1 1) (catch Exception e (pr-str e)))
          ex-str2 (try (f2 1) (catch Exception e (pr-str e)))]
      (is (cstr/includes? ex-str1 "foobarbaz"))
      (is (cstr/includes? ex-str2 "unnamed_database_function"))))
  (testing "installed-functions"
    (with-local-mem-connection conn
      (let [_ @(api/transact conn [{:db/ident :my-fn
                                    :db/id    (api/tempid :db.part/db)
                                    :db/fn    (api/function {:lang    :clojure
                                                             :fn-name 'override-name
                                                             :params  '[x]
                                                             :code    '(throw (Exception. "foo!"))})}])
            ex-str1 (try @(api/transact conn [[:my-fn]]) (catch Exception e (pr-str e)))
            ex-str2 (try @(api/transact conn [[:db.fn/cas 0 9 "foo" "bar"]]) (catch Exception e (pr-str e)))]
        (testing "if both a fn-name and db/ident are associated with a function, use fn-name"
          (is (cstr/includes? ex-str1 "override_name")))
        (testing "if there is only a db/ident, munge the keyword into a valid fn-name"
          (is (cstr/includes? ex-str2 "db_DOT_fn_SLASH_cas")))))))
