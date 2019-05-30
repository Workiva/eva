// Copyright 2015-2019 Workiva Inc.
// 
// Licensed under the Eclipse Public License 1.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//      http://opensource.org/licenses/eclipse-1.0.php
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eva;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Utility functions for working with eva and Clojure from Java.
 */
public final class Util {
    private static IFn apply = Clojure.var("clojure.core", "apply");
    private static IFn vec = Clojure.var("clojure.core", "vec");
    private static IFn hashMap = Clojure.var("clojure.core", "hash-map");
    private static IFn arrayMap = Clojure.var("clojure.core", "array-map");
    private static IFn name = Clojure.var("clojure.core", "name");
    private static IFn namespace = Clojure.var("clojure.core", "namespace");
    private static IFn cljTransient = Clojure.var("clojure.core", "transient");
    private static IFn persistentBang = Clojure.var("clojure.core", "persistent!");
    private static IFn assocBang = Clojure.var("clojure.core","assoc!");


    private Util () {}

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("eva.api.java.util"));
    }

    /**
     * Create a Clojure vector containing items.
     *
     * @param items Items to add to the vector
     * @return A clojure vector containing the items given
     */
    public static List list(Object... items) {
        return (List) vec.invoke(items);
    }

    /**
     * Create a Clojure map from keyVals.
     *
     * @param keyVals Keys and values to construct the map with
     * @return A Clojure map
     */
    public static Map map(Object... keyVals) {
        if(keyVals.length <= 8) {
            return (Map)apply.invoke(arrayMap, keyVals);
        } else {
            return (Map)apply.invoke(hashMap, keyVals);
        }
    }

    /**
     * Create a Clojure map of keywords to value from keyVals.
     *
     * @param keyVals Keys and values to construct the map with
     * @return A Clojure map
     */
    @SuppressWarnings("unchecked")
    public static Map<Keyword, Object> keywordMap(Object... keyVals) {
        assert(keyVals.length % 2 == 0);
        Object map;
        if(keyVals.length <= 16) {
            map = cljTransient.invoke(arrayMap.invoke());
        } else {
            map = cljTransient.invoke(hashMap.invoke());
        }
        for(int i=0; i < keyVals.length; i+=2) {
            Keyword k = Keyword.intern((String) keyVals[i]);
            map = assocBang.invoke(map, k, keyVals[i+1]);
        }
        return (Map) persistentBang.invoke(map);
    }

    /**
     * Read the name field from a Clojure keyword.
     *
     * <p>E.g., name(:namespace/name) → name
     *
     * @param k A Clojure keyword
     * @return The name portion of the keyword
     */
    public static String name(Object k) {
        return (String)name.invoke(k);
    }

    /**
     * Read the namespace field from a Clojure keyword, if it exists.
     *
     * <p>E.g., name(:namespace/name) → namespace
     *
     * @param k A Clojure keyword
     * @return The namespace portion of the keyword
     */
    public static String namespace(Object k) {
        return (String)namespace.invoke(k);
    }

    /**
     * Convert an input string into an EDN element.
     *
     * @param source An EDN element as a string
     * @return The EDN element converted from the string
     */
    public static Object read(String source) {
        IFn read = Clojure.var("eva.api.java.util", "read");
        return read.invoke(source);
    }

    /**
     * Convert an input string into an EDN element.
     *
     * @param readers Custom Clojure readers to parse custom EDN elements
     * @param source An EDN element as a string
     * @return The EDN element converted from the string
     */
    public static Object read(Map<String,Function> readers, String source) {
        IFn read = Clojure.var("eva.api.java.util", "read");
        return read.invoke(readers, source);
    }

    private static List readAllImpl(Object reader) {
        IFn readAll = Clojure.var("eva.api.java.util", "read-all");
        return (List)readAll.invoke(reader);
    }

    /**
     * Load a list of EDN elements from a character stream.
     *
     * @param reader Input stream of EDN elements
     * @return A list of EDN elements read from the input stream
     */
    public static List readAll(Reader reader) {
        return readAllImpl(reader);
    }

    /**
     * Read a file into a list of EDN elements.
     *
     * @param file {@link java.io.File} containing EDN elements.
     * @return A list of EDN elements read from {@code file}
     */
    public static List readAll(File file) {
        return readAllImpl(file);
    }

    /**
     * Read the resource at uri into a list of EDN elements.
     *
     * @param uri {@link java.net.URI} which links to EDN elements
     * @return A list of EDN elements read from source {@code uri}
     */
    public static List readAll(URI uri) {
        return readAllImpl(uri);
    }

    /**
     * Read the resource at url into a list of EDN elements.
     *
     * @param url {@link java.net.URL} which links to EDN elements
     * @return A list of EDN elements read from source {@code url}
     */
    public static List readAll(URL url) {
        return readAllImpl(url);
    }

    /**
     * Load a list of EDN elements from an input stream.
     *
     * @param in Input stream of EDN elements
     * @return A list of EDN elements read from the input stream
     */
    public static List readAll(InputStream in) {
        return readAllImpl(in);
    }
}
