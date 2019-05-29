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

(ns eva.server.v1.http-status-endpoint
  (:require [com.stuartsierra.component :as component]
            [clojure.pprint :refer [pprint]]
            [barometer.core :as metrics])
  (:import (com.sun.net.httpserver HttpServer HttpHandler HttpExchange)
           (java.net InetSocketAddress)))

(defn default-status-code [_] 503)

(def heartbeat
  (metrics/get-or-register metrics/DEFAULT 'eva.server.heartbeat
                           (metrics/counter "Counts calls to handle-status-response.")))

(defn handle-status-response
  ([^HttpExchange he data] (handle-status-response he default-status-code data))
  ([^HttpExchange he status-code data]
   (try
     (metrics/increment heartbeat)
     (let [body (str (with-out-str (pprint data)))]
       (doto (.getResponseHeaders he)
         (.put "Content-Type" ["application/edn; charset=utf-8"]))
       (.sendResponseHeaders he (status-code data) (.length body))
       (with-open [os (.getResponseBody he)]
         (.write os (.getBytes body "UTF-8")))))))

(defrecord StatusServer [host port data-fn]
  component/Lifecycle
  (start [{:as this :keys [http-server status-code]}]
    (cond-> this
            (not http-server) (assoc :http-server
                                     (doto (HttpServer/create (InetSocketAddress. host port) 0)
                                       (.createContext "/status" (reify HttpHandler
                                                                   (handle [_ hx]
                                                                     (handle-status-response hx
                                                                                             (or status-code
                                                                                                 default-status-code)
                                                                                             (data-fn)))))
                                       (.setExecutor nil)
                                       (.start)))))
  (stop [{:as this :keys [http-server]}]
    (cond-> this
            http-server (as-> this (do (.stop http-server 0)
                                       (dissoc this :http-server))))))

(defn status-server
  ([host port data-fn] (status-server host port data-fn))
  ([host port status-code data-fn]
   (component/start (map->StatusServer {:host host :port port :status-code status-code :data-fn data-fn}))))
