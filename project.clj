(defproject com.workiva/eva "2.0.4"
  :description "A distributed database-system implementing an entity-attribute-value data-model that is time-aware, accumulative, and atomically consistent"
  :url "https://github.com/Workiva/eva"
  :license {:name "Eclipse Public License 1.0"}
  :plugins [[lein-shell "0.5.0"]
            [lein-codox "0.10.8"]]
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [backtick/backtick "0.3.4"]
                 [org.clojure/tools.macro "0.1.5"]
                 [potemkin/potemkin "0.4.5"]
                 [prismatic/schema "1.2.0"]
                 [prismatic/plumbing "0.6.0"]
                 [org.clojure/data.avl "0.1.0"]
                 [com.rpl/specter "0.12.0"]
                 [com.google.guava/guava "22.0"]
                 [manifold/manifold "0.1.6"]
                 [org.apache.activemq/artemis-core-client "2.4.0"]
                 [org.apache.activemq/artemis-jms-client "2.4.0"]
                 [org.apache.activemq/artemis-server "2.4.0"]
                 [org.apache.activemq/artemis-jms-server "2.4.0"]
                 [org.apache.activemq/artemis-openwire-protocol "2.4.0"
                  :exclusions [org.apache.geronimo.specs/geronimo-jms_1.1_spec]]
                 [org.apache.activemq/activemq-client "5.15.8"
                  ;; exclude jms 1.1 api classes that conflict with jms 2.0 api classes
                  :exclusions [org.apache.geronimo.specs/geronimo-jms_1.1_spec]]
                 [expound/expound "0.7.1"]
                 [com.stuartsierra/component "1.1.0"]
                 [org.slf4j/slf4j-api "1.7.36"]
                 [org.clojure/core.cache "0.6.5"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/core.memoize "0.5.9"]
                 [listora/again "1.0.0"]
                 [funcool/cats "2.4.2"]
                 ;;^:source-dep [clj-radix "0.1.0"]

                 [org.clojure/tools.cli "0.3.7"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.clojure/core.unify "0.5.7"]
                 [com.mchange/c3p0 "0.9.5.5"]
                 [com.h2database/h2 "1.4.197"]
                 [org.xerial/sqlite-jdbc "3.36.0.3"]
                 [org.mariadb.jdbc/mariadb-java-client "1.4.6"]
                 [mysql/mysql-connector-java "5.1.45"]
                 [org.postgresql/postgresql "9.4-1206-jdbc42"]
                 [org.clojure/math.numeric-tower "0.0.5"]
                 [aysylu/loom "1.0.2" :exclusions [tailrecursion/cljs-priority-map]]
                 [org.clojure/data.fressian "1.0.0"]
                 [com.carrotsearch/java-sizeof "0.0.5"]
                 [joda-time/joda-time "2.10.14"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.18" :exclusions [joda-time
                                                                             commons-logging/commons-logging]]
                 [io.jaegertracing/jaeger-client "0.34.0"]
                 [io.opentracing/opentracing-api "0.32.0"]
                 [org.clojure/java.jmx "0.3.3"]
                 [com.workiva/utiliva "0.2.1"]
                 [map-experiments/map-experiments "0.5.0-SNAPSHOT"]
                 [com.workiva/recide "1.0.1"]
                 [com.workiva/tesserae "1.0.1"]
                 [com.workiva/ichnaie "0.1.2"]
                 [com.workiva/barometer "0.1.2"]
                 [com.workiva/flowgraph "0.1.1"]
                 [galdre/morphe "1.2.0"]
                 [com.workiva/quartermaster "0.1.1"]]

  :deploy-repositories {"clojars"
                        {:url "https://repo.clojars.org"
                         :username :env/clojars_username
                         :password :env/clojars_password
                         :sign-releases false}}

  :source-paths      ["core/src"]
  :java-source-paths ["core/java-src"]
  :test-paths        ["core/test"]
  :resource-paths    ["core/resources"]

  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  ;; newratio flag determined by running a number of benchmarks designed to imitate a motivating workload
  ;; 3 in all cases was superior to the JVM default of 2, sometimes with a performance difference ~10-15%. (v0.3.99)
  :jvm-opts ^:replace ["-XX:-OmitStackTraceInFastThrow" "-XX:+StartAttachListener" "-XX:NewRatio=3"
                       ;;"-agentpath:/Applications/YourKit-Java-Profiler-2019.1.app/Contents/Resources/bin/mac/libyjpagent.jnilib"
                       "-XX:+UseG1GC" "-XX:MaxGCPauseMillis=50"]

  :aliases {"mariadb-test-server"     ["shell" "dev/integration-testing/mariadb/start-test-database.sh"]
            "latest-release-version"  ["shell" "git" "describe" "--match" "v*.*" "--abbrev=0"]
            "java-api-docs"           ["shell" "javadoc" "-d" "./docs/api/java" "-notimestamp"
                                       "core/java-src/eva/Attribute.java"
                                       "core/java-src/eva/Connection.java"
                                       "core/java-src/eva/Database.java"
                                       "core/java-src/eva/Datom.java"
                                       "core/java-src/eva/Entity.java"
                                       "core/java-src/eva/Id.java"
                                       "core/java-src/eva/Log.java"
                                       "core/java-src/eva/Peer.java"
                                       "core/java-src/eva/Util.java"
                                       "core/java-src/eva/error/v1/EvaErrorCode.java"
                                       "core/java-src/eva/error/v1/EvaException.java"
                                       "core/java-src/eva/error/v1/ICodedExceptionInfo.java"
                                       "core/java-src/eva/error/v1/IErrorCode.java"]
            "gen-config-table"        ["run" "-m" "eva.config/help" "docs/api/eva_config_properties.md"]
            "docs"                    ["do" "clean-docs," "java-api-docs," "with-profile" "api-docs" "codox," "gen-config-table"]
            "clean-docs"              ["shell" "rm" "-rf" "./docs/api"]
            "internal-api-docs"       ["do" "clean-internal-api-docs," "with-profile" "internal-api-docs" "codox"]
            "generate-error-codes"    ["do"
                                       "run" "-m" "eva.dev.tasks.errorcode-generation/delete-previous-file,"
                                       "run" "-m" "eva.dev.tasks.errorcode-generation/generate-error-code-file"]}

  :codox {:metadata {:doc/format :markdown}
          :themes [:rdash]
          :html {:transforms [[:title]
                              [:substitute [:title "EVA API Docs"]]
                              [:span.project-version]
                              [:substitute nil]
                              [:pre.deps]
                              [:substitute [:a {:href "https://clojars.org/com.workiva/eva"}
                                            [:img {:src "https://img.shields.io/clojars/v/com.workiva/eva.svg"}]]]]}
          :namespaces  [eva.api]
          :output-path "docs/api/clojure"}

  :profiles {:logback         {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}
             :logging-bridges {:dependencies [[org.slf4j/jcl-over-slf4j "1.7.12"]
                                              [org.slf4j/log4j-over-slf4j "1.7.12"]]}

             :server {:dependencies      [[ch.qos.logback/logback-classic "1.2.3"]
                                          [org.apache.derby/derby "10.14.1.0"]
                                          [org.clojure/tools.cli "0.3.5"]
                                          [com.workiva.eva.catalog/client.alpha "2.0.1"]]
                      :source-paths      ["server/src"]
                      :java-source-paths ["server/java-src"]
                      :test-paths        ["server/tests"]}

             :server-main [:server :aot
                           {:main ^:skip-aot eva.Server
                            :aot  [eva.server.v2]}]

             :aot {:aot [eva.api]}

             :activemq-client {:dependencies [[org.apache.activemq/activemq-client "5.15.8"
                                               ;; exclude jms 1.1 api classes that conflict with jms 2.0 api classes
                                               :exclusions [org.apache.geronimo.specs/geronimo-jms_1.1_spec]]]}

             :activemq-server {:dependencies [[org.apache.activemq/activemq-broker "5.15.8"]
                                              [org.apache.activemq/activemq-kahadb-store "5.15.3"]]}

             :dev [:server :activemq-client :activemq-server :logback :logging-bridges
                   {:dependencies      [[criterium "0.4.3"]
                                        [clojure-csv/clojure-csv "2.0.1"]
                                        [com.gfredericks/test.chuck "0.1.19"]
                                        [org.clojure/test.check "0.10.0-alpha2"]
                                        [org.clojure/data.generators "0.1.2"]
                                        [org.clojure/tools.namespace "0.2.11"]
                                        [com.googlecode.log4jdbc/log4jdbc "1.2"]
                                        [org.clojure/tools.namespace "0.2.11"]
                                        [philoskim/debux "0.5.2"]
                                        [circleci/bond "0.3.1"]
                                        [vvvvalvalval/scope-capture "0.3.2"]
                                        [com.mockrunner/mockrunner-jms "1.1.2" :exclusions [commons-logging/commons-logging]]]
                    :injections        [(require 'debux.core)
                                        (require 'sc.api)]
                    :source-paths      ["dev/src"]
                    :java-source-paths ["dev/java-src"]
                    :test-paths        ["dev/test"]
                    :resource-paths    ["dev/resources" "core/test-resources" "server/test-resources"]
                    :repl-options      {:init-ns eva.dev.repl :timeout 120000}
                    :test-selectors    {:default (complement :slow)
                                        :slow    :slow}
                    :global-vars       {*warn-on-reflection* true}}
                   {:plugins [[jonase/eastwood "0.3.5"]]}]

             :dynamodb-local     {:repositories [["dynamodb-local" "http://dynamodb-local.s3-website-us-west-2.amazonaws.com/release"]]
                                  :dependencies [[com.amazonaws/DynamoDBLocal "1.11.0.1" :exclusions [com.amazonaws/aws-java-sdk-dynamodb
                                                                                                      org.apache.logging.log4j/log4j-core]]]}
             :jar                {:global-vars {*warn-on-reflection* false}}
             :uberjar            [:server-main {:uberjar-name "transactor.jar"
                                                :global-vars  {*warn-on-reflection* false}}]

             :deployment [:logback :logging-bridges
                          {:resource-paths ["server/src/resources"]}]

             :api-docs {:dependencies [[codox-theme-rdash "0.1.2"]]}

             :internal-api-docs {:dependencies [[codox-theme-rdash "0.1.2"]]
                                 :codox        ^:replace {:output-path "./docs/api/"}}

             :debug-compile {:javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options" "-g"]}})
