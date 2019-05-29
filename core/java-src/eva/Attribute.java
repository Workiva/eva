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

/**
 * The Attribute interface provides a programatic representation of a schema attribute.
 *
 * <p>Attributes are loaded into memory, and it is generally more efficient to access
 * attribute information via this interface.
 */
public interface Attribute {
    Object CARDINALITY_MANY = Clojure.read(":db.cardinality/many");
    Object CARDINALITY_ONE = Clojure.read(":db.cardinality/one");
    Object TYPE_BIGDEC = Clojure.read(":db.type/bigdec");
    Object TYPE_BIGINT = Clojure.read(":db.type/bigint");
    Object TYPE_BOOLEAN = Clojure.read(":db.type/boolean");
    Object TYPE_BYTES = Clojure.read(":db.type/bytes");
    Object TYPE_DOUBLE = Clojure.read(":db.type/double");
    Object TYPE_FLOAT = Clojure.read(":db.type/float");
    Object TYPE_FN = Clojure.read(":db.type/fn");
    Object TYPE_INSTANT = Clojure.read(":db.type/instant");
    Object TYPE_KEYWORD = Clojure.read(":db.type/keyword");
    Object TYPE_LONG = Clojure.read(":db.type/long");
    Object TYPE_REF = Clojure.read(":db.type/ref");
    Object TYPE_STRING = Clojure.read(":db.type/string");
    Object TYPE_URI = Clojure.read(":db.type/uri");
    Object TYPE_UUID = Clojure.read(":db.type/uuid");
    Object UNIQUE_IDENTITY = Clojure.read(":db.unique/identity");
    Object UNIQUE_VALUE = Clojure.read(":db.unique/value");

    /**
     * Provides the cardinality of the attribute.
     *
     * @return either {@link #CARDINALITY_ONE} or {@link #CARDINALITY_MANY}
     */
    Object cardinality();

    /**
     * Unsupported.
     *
     * @return true if attribute has :db/fulltext true
     */
    boolean hasFullText();

    /**
     * Unsupported.
     *
     * @return true if attribute has :db/noHistory true
     */
    boolean hasNoHistory();

    /**
     * Entity id of the attribute.
     *
     * @return entity id
     */
    Object id();

    /**
     * Human readable keyword that identifies the attribute.
     *
     * @return keyword identifier
     */
    Object ident();

    /**
     * Tests if the reference attribute is a component-attribute.
     *
     * <p>This indicates that the referenced entity is considered "a part of"
     * the referencing entity.
     *
     * @return true if attribute has :db/isComponent true
     */
    boolean isComponent();

    /**
     * Indicates if the attribute value are indexed.
     *
     * <p>This allows efficient searches by attribute value.
     * By default, all attributes are indexed (except when of type <code>:db.type/bytes</code>)
     *
     * @return true if attribute DOES NOT have :db/index false or :db/valueType :db.type/bytes
     */
    boolean isIndexed();

    /**
     * Indicates if the attribute is in the AVET index.
     * Unlike Datomic, this is always true if isIndexed() is true.
     *
     * @return true if isIndexed() is true
     */
    boolean hasAVET();

    /**
     * Indicates if the attribute is unique, and if so, what type of uniqueness constraint is enforced.
     *
     * @return either {@link #UNIQUE_IDENTITY} or {@link #UNIQUE_VALUE}
     */
    Object unique();


    /**
     * The value type of the attribute.
     *
     * @return one of: {@link #TYPE_BIGDEC} {@link #TYPE_BIGINT} {@link #TYPE_BOOLEAN} {@link #TYPE_BYTES}
     *                 {@link #TYPE_DOUBLE} {@link #TYPE_FLOAT} {@link #TYPE_FN} {@link #TYPE_INSTANT}
     *                 {@link #TYPE_KEYWORD} {@link #TYPE_LONG} {@link #TYPE_REF} {@link #TYPE_STRING}
     *                 {@link #TYPE_URI} {@link #TYPE_UUID}
     */
    Object valueType();

}
