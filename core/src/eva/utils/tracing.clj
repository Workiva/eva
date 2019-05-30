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

(ns eva.utils.tracing
  (:require [eva.config :refer [config-strict]]
            [utiliva.macros :refer [when-class]]
            [ichnaie.core :refer [tracing-with-spanbuilder]])
  (:import [io.jaegertracing Configuration]
           [io.jaegertracing Configuration$ReporterConfiguration]
           [io.jaegertracing Configuration$SamplerConfiguration]
           [io.opentracing SpanContext Span Tracer]))

(defn construct-tracer
  []
  (let [config (doto (Configuration. "eva")
                 ;; sampler always makes the same decision for all traces ("const") - trace all (1)
                 ;; See https://static.javadoc.io/io.jaegertracing/jaeger-core/0.34.0/io/jaegertracing/Configuration.html for
                 ;; more details about configuration classes
                 (.withSampler (.. Configuration$SamplerConfiguration (fromEnv) (withType "const") (withParam 1)))
                 (.withReporter (.. Configuration$ReporterConfiguration (fromEnv))))]
    (.getTracer config)))

(defn trace-fn-with-tags [span-name f & args]
  (if-let [tracer ^Tracer ichnaie.core/*tracer*]
    (let [span-builder (as-> (.buildSpan tracer span-name) span-builder ;; make span-builder
                             (if (some? ichnaie.core/*trace-stack*) ;; maybe make it a child
                               (.asChildOf span-builder ^Span ichnaie.core/*trace-stack*)
                               span-builder)
                             (if (config-strict :eva.tracing.tag-inputs)
                               (reduce-kv (fn [span-builder position arg] ;; tag it
                                            (.withTag span-builder (str position) (str arg)))
                                          span-builder
                                          (vec args))
                               span-builder))]
      (tracing-with-spanbuilder span-builder
                                (apply f args)))
    (apply f args)))
