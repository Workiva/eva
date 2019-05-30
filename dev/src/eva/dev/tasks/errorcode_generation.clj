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

(ns eva.dev.tasks.errorcode-generation
  (:require [clojure.java.io :as io])
  (:import [java.nio.file Files Path Paths FileVisitOption LinkOption]))

(def enum-template
  "// Copyright 2015-2019 Workiva Inc.
// 
// Licensed under the Eclipse Public License 1.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://opensource.org/licenses/eclipse-1.0.php
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eva.error.v1;

import java.util.HashMap;
import java.util.Map;
import clojure.lang.Keyword;

public enum EvaErrorCode implements IErrorCode {

%s;

    private EvaErrorCode parent;
    private long evaCode;
    private long httpCode;
    private String name;
    private Keyword keyword;
    private String explanation;

    // https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
    private static class Mapping {
        static Map<Long, EvaErrorCode> CODE_TO_CODE = new HashMap<>();
    }

    private EvaErrorCode(EvaErrorCode parent,
                         long evaCode,
                         long httpCode,
                         String name,
                         Keyword keyword,
                         String explanation) {
        this.parent = parent;
        this.evaCode = evaCode;
        this.httpCode = httpCode;
        this.name = name;
        this.keyword = keyword;
        // this.keyword = Keyword.intern(keyword);
        this.explanation = explanation;
        Mapping.CODE_TO_CODE.put(evaCode, this);
    }

    public static EvaErrorCode getFromLong(Long code) {
        return Mapping.CODE_TO_CODE.get(code);
    }

    @Override
    public boolean isUnspecified() {
        return evaCode == -1;
    }

    /*
     * -1 is used by UnknownError.
     * Long.MIN_VALUE is used by all non-terminal error codes.
     * The intention is that these error codes will never be returned directly,
     * but are used merely to provide a sensible hierarchy for `is()` calls.
    */
    @Override
    public long getCode() {
        return evaCode;
    }

    @Override
    public long getHttpErrorCode() {
        return httpCode;
    }

    @Override
    public String getScope() {
        if (parent == null) {
            return \"\";
        } else {
            return parent.getScope() + parent.getName() + \": \";
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Keyword getKey() {
        return keyword;
    }

    @Override
    public String getExplanation() {
        return explanation;
    }

    @Override
    public boolean is(IErrorCode e) {
        return (this == e) ? true :
               (parent == null) ? false :
               parent.is(e);
    }

}
")

(defn- iterate-until-fixed
  ([f x]
   (let [r (f x)]
     (if (= r x)
       r
       (recur f r)))))

(defn- replace-step
  [s]
  (clojure.string/replace-first
   s
   #"([^A-Z_])([A-Z])|([A-Z])([A-Z])([a-z])"
   (fn [[_ x1 x2 y1 y2 y3]]
     (if x1
       (format "%s_%s" x1 x2)
       (format "%s_%s%s" y1 y2 y3)))))

(defn- camelcase->snakecase
  [sym]
  (->> (str sym)
       (iterate-until-fixed replace-step)
       clojure.string/upper-case))

(def indent "    ")
(defn make-indent
  [s]
  (apply str (repeat (count (str s)) \ )))

(def constructor-template
  (format
   "%s%s%s(%s,
 %s%s%s%s,
 %s%s%s%s,
 %s%s%s\"%s\",
 %s%s%sKeyword.intern(\"%s\",
 %s%s%s%s\"%s\"),
 %s%s%s\"%s\")"
   "%s"
   indent
   "%s"
   "%s"
   "%s"
   indent
   "%s"
   "%s"
   "%s"
   indent
   "%s"
   "%s"
   "%s"
   indent
   "%s"
   "%s"
   "%s"
   indent
   "%s"
   "%s"
   "%s"
   indent
   (make-indent "Keyword.intern(")
   "%s"
   "%s"
   "%s"
   indent
   "%s"
   "%s"
   "%s"))

(defn- fill-constructor-template
  [hier-indent name-indent enum parent-enum code http-status enum-name kw-ns kw-name expl]
  (format constructor-template
          hier-indent
          enum
          parent-enum
          hier-indent
          name-indent
          code
          hier-indent
          name-indent
          http-status
          hier-indent
          name-indent
          enum-name
          hier-indent
          name-indent
          kw-ns
          hier-indent
          name-indent
          kw-name
          hier-indent
          name-indent
          expl))

(defn- generate-enum-def
  [level parent {:keys [sym code http-status keyword doc]}]
  (let [parent-ref (if parent
                     (camelcase->snakecase (:sym parent))
                     "null")
        hier-indent (apply str (repeat level indent))
        name-indent (make-indent (camelcase->snakecase sym))]
    (fill-constructor-template hier-indent
                               name-indent
                               (camelcase->snakecase sym)
                               parent-ref
                               code
                               http-status
                               (str sym)
                               (namespace keyword)
                               (name keyword)
                               doc)))

(defn- make-kw
  [top-level-keyword raw-kw]
  (if top-level-keyword
    (keyword (or (namespace top-level-keyword)
                 (name top-level-keyword))
             (name raw-kw))
    (if (nil? (namespace raw-kw))
      (keyword "eva.error" (name raw-kw))
      raw-kw)))

(defn- step
  [level
   top-level-keyword
   {:as parent}
   [sym raw-subtype]]
  (let [raw-kw (:keyword raw-subtype)
        kw (make-kw top-level-keyword raw-kw)
        doc (:doc raw-subtype)
        ;; Long/MIN_VALUE used as code for non-terminal types.
        ;; These types are intended never to be used by Eva directly.
        ;; The exist to provide a sensible hierarchy for is() checks.
        eva-code (:code raw-subtype "Long.MIN_VALUE")
        http-status (get raw-subtype :http-status
                         (get parent :http-status 500))
        subtypes (:subtypes raw-subtype)
        ;; asserts here
        this {:sym sym,
              :code eva-code,
              :http-status http-status,
              :keyword kw,
              :doc doc}]
    (->> this
         (generate-enum-def level parent)
         (conj (if subtypes
                 (mapcat (partial step
                                  (inc level)
                                  (or top-level-keyword raw-kw)
                                  this)
                         subtypes)
                 ())))))

(def error-codes
  (clojure.core/read-string (slurp "core/resources/eva/error/error_codes.edn")))

(defn- delete-previous-file
  []
  (let [file (Paths/get "core/java-src/eva/error/v1/EvaErrorCode.java" (make-array String 0))]
    (Files/delete file)))

(defn- make-file-string
  []
  (format enum-template
          (clojure.string/join ",\n" (mapcat (partial step 0 nil nil) error-codes))))

(defn generate-error-code-file
  []
  (spit "core/java-src/eva/error/v1/EvaErrorCode.java" (make-file-string)))

(defn check-equivalence
  []
  (=
   (slurp "core/java-src/eva/error/v1/EvaErrorCode.java")
   (make-file-string)))
