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

import java.util.List;
import java.util.Map;

public interface Database {
    /**
     * Identifies the EAVT (entity-attribute-value-transaction) index.
     *
     * <p>Pass to methods that consume an index name like {@link #datoms(Object, Object...)}
     */
    Object EAVT = Clojure.read(":eavt");

    /**
     * Identifies the AEVT (attribute-value-entity-transaction) index.
     *
     * <p>Pass to methods that consume an index name like {@link #datoms(Object, Object...)}
     */
    Object AVET = Clojure.read(":avet");

    /**
     * Identifies the AEVT (attribute-entity-value-transaction) index.
     *
     * <p>Pass to methods that consume an index name like {@link #datoms(Object, Object...)}
     */
    Object AEVT = Clojure.read(":aevt");

    /**
     * Identifies the VAET (value-attribute-entity-transaction) index.
     *
     * <p>Pass to methods that consume an index name like {@link #datoms(Object, Object...)}
     */
    Object VAET = Clojure.read(":vaet");

    /**
     * Retrieves information about an Attribute.
     *
     * @param attrId entity identifier (entity-id or ident-keyword) of an attributee
     * @return an {@link Attribute} if one exists
     */
    Attribute attribute(Object attrId);

    /**
     * Retrieves information about an Attribute.
     *
     * @param attrId entity identifier (entity-id or ident-keyword) of an attributee
     * @return an {@link Attribute} if one exists, if not, throws.
     */
    Attribute attributeStrict(Object attrId);

    /**
     * The transaction id of the most recent transaction in this database.
     *
     * @return a transaction id
     */
    long basisT();

    /**
     * The next transaction id that will be assigned in this database.
     *
     * @return a transaction id
     */
    long nextT();

    /**
     * Provides raw access to the database indexes.
     *
     * <p>Must pass the index-name. May pass one or more leading components of the index
     * to constrain the results.
     *
     * <p>The following indexes may be searched:
     * <dl>
     * <dd>{@link #EAVT}</dd> <dt>contains all datoms sorted by entity-id, attribute-id, value, and transaction</dt>
     * <dd>{@link #AEVT}</dd> <dt>contains all datoms sorted by attribute-id, entity-id, value, and transaction</dt>
     * <dd>{@link #AVET}</dd> <dt>contains datoms with indexed and unique attributes (except for bytes values)
     *                            sorted by attribute-id, value, entity-id, and transaction</dt>
     * <dd>{@link #VAET}</dd> <dt>contains datoms with attributes of :db.type/ref; VAET acts as the reverse index
     *                            for backwards traversal of refs</dt>
     * </dl>
     *
     * @param index one of: {@link #EAVT}, {@link #AEVT}, {@link #AVET}, {@link #VAET}
     * @param components any datom components (entity-id, attribute, value, or transaction) specified in the order
     *                   of the chosen index
     * @return datoms in the chosen index which match the components
     */
    Iterable<Datom> datoms(Object index, Object... components);


    /**
     * Returns true if there exists at least one datom in the database with the provided
     * entity identifier.
     *
     * @param entityId an entity-identifier
     * @return boolean indicating if the entity exists.
     */
    Boolean isExtantEntity(Object entityId);

    /**
     * Coerces any entity-identifier into an entity-id. Does *not* confirm existence
     * of an entity id, except incidentally through some coercion processes.
     *
     * <p>To check for existence of an entity, please use {@link #isExtantEntity(Object)}
     *
     * @param entityId an entity-identifier
     * @return id or nil if entityId cannot be coerced.
     */
    Object entid(Object entityId);

    /**
     * Like entid but operates in a batch. Prefer this method if you need to coerce multiple ids.
     * @param entityIds a list of entityIds or entity-identifiers (keywords or lookup references)
     * @return a list (in the same order) of ids or nil if no id exists
     */
    Object entids(List<Object> entityIds);

    /**
     * Behaves the same as entid, but instead throws where entid would return nil.
     */
    Object entidStrict(Object entityId);

    /**
     * Analogy -- entid : entidStrict :: entids entidsStrict.
     */
    Object entidsStrict(List<Object> entityIds);

    /**
     * Returns an {@link Entity}, which is a dynamic, lazy-loaded projection of the datoms that share the same entity-id.
     *
     * @param entityId entity identifier
     * @return an {@link Entity}
     */
    Entity entity(Object entityId);

    /**
     * Returns the keyword-identifier associated with an id.
     *
     * @param idOrKey entity-id or keyword
     * @return keyword identifier or nil if doesn't exists
     */
    Object ident(Object idOrKey);
    /**
     * Returns the keyword-identifier associated with an id.
     * Throws an exception if none exists.
     *
     * @param idOrKey entity-id or keyword
     * @return keyword identifier
     */
    Object identStrict(Object idOrKey);

    /**
     * If this database isn't the latest snapshot at the time it was constructed, return the
     * {@code tx-num} of the log entry / transaction this database was constructed from to reflect;
     * else null.
     *
     * @return null if this database is the latest snapshot; the tx-num of the latest log entry elsewise.
     */
    Long asOfT();

    /**
     * Yields the most recently known database snapshot, which is maintained in memory.
     */
    Long snapshotT();

    /**
     * Returns a historical database value at some point in time {@code t}, inclusive.
     *
     * @param t A transaction number or id.
     * @return The database value at point in time {@code t}
     */
    Database asOf(Object t);

    /**
     * Looks up database function identified by entityId and invokes the function
     * with args.
     *
     * @param entityID entity-identifier of function entity
     * @param args args to pass to function
     * @return value returned by invoking function
     */
    Object invoke(Object entityID, Object... args);

    /**
     * Executes a pull-query returning a hierarchical selection of attributes for entityId.
     *
     * @param pattern a pattern or a string containing a pattern in EDN format
     * @param entityId an entity-identifier
     * @return a map containing the hierarchical selection requested
     */
    java.util.Map pull(Object pattern, Object entityId);

    /**
     * Returns multiple hierarchical selections for the passed entity-identifiers.
     *
     * @param pattern a pattern or a string containing a pattern in EDN format
     * @param entityIds list of entity-identifiers
     * @return list of maps contains the hierarchical selections requested
     */
    List<Map> pullMany(Object pattern, List entityIds);

    /**
     * Returns a special view of the database containing *all* datoms, asserted
     * or retracted across time. Currently supports the 'datoms' and 'q'
     * functions.
     *
     * @return a database containing all datoms across time.
     */
    Database history();

    /**
     * Simulates a transaction locally without persisting the updated state.
     *
     * @param txData a list of transaction data, follows the same format as the
     *        argument to 'transact'
     * @return a map with the same contents as the future from 'transact'
     */
    Map with(java.util.List txData);


}
