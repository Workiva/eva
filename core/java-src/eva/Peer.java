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

import java.util.*;
import java.util.function.Function;

public final class Peer {
    private static IFn require = Clojure.var("clojure.core", "require");
    private static IFn seq = Clojure.var("clojure.core", "seq");
    private static IFn apply = Clojure.var("clojure.core", "apply");

    // This static block is executed at class-load-time.
    // TAKE CARE when adding slow-running code here, as this
    // will cause class-loading to block, giving the appearance of
    // slow startup.
    // Only `require` clojure namespaces which are fast to load.
    static {
        // Require in the utiliva.uuid namespace which
        // provides squuid generation, is small, has
        // no other clojure dependencies, and is fast to load.
        require.invoke(Clojure.read("utiliva.uuid"));
    }

    private Peer () {}

    // Provides lazy/deferred loading of the eva.api namespace
    private static volatile boolean evaApiWasRequired = false;
    private static void requireEvaApi() {
        if(!evaApiWasRequired) {
            synchronized (Peer.class) {
                require.invoke(Clojure.read("eva.api"));
                evaApiWasRequired = true;
            }
        }
    }

    /**
     * Returns a {@link eva.Connection}, given a valid connection configuration map.
     *
     * @param map An eva connection configuration map.
     * @return A connection
     */
    public static Connection connect(Object map) {
        requireEvaApi();
        IFn connect = Clojure.var("eva.api", "connect");
        return (Connection) connect.invoke(map);
    }

    /**
     * Execute a query against the given inputs.
     *
     * @param query An eva query
     * @param inputs Inputs to the query
     * @param <T> expected return type (depends on find-spec)
     * @return Query results
     */
    @SuppressWarnings("unchecked")
    public static <T> T query(Object query, Object... inputs) {
        requireEvaApi();
        IFn q = Clojure.var("eva.api", "q");
        return (T) apply.invoke(q, query, seq.invoke(inputs));
    }

    /**
     * Resolves a temporary-id to a permanent id given a database and a mapping of tempid to permids.
     *
     * <p> Intended for use with the results of a {@link eva.Connection#transact(List)} call</p>
     *
     * <pre>
     * {@code
     *  long tempid = Peer.tempid(...);
     *  Connection conn = Peer.connect(...);
     *  Map item = new HashMap() {{
     *      put(Keyword(":db/id", tempid);
     *      ...;
     *  }};
     *  List tx = new ArrayList() {{
     *      add(item);
     *      ...;
     *  }};
     *  Map result = conn.transact(tx).get();
     *
     *  Object resolvedId = Peer.resolveTempId(result.get(Connection.DB_AFTER),
     *                                         result.get(Connection.TEMPIDS),
     *                                         tempid);
     * }
     * </pre>
     *
     * @param db An eva database
     * @param tempids A map of tempids returned from a transaction
     * @param tempid A temporary id to resolve to its permanent id
     * @return The resolved permanent id
     */
    public static Object resolveTempId(Database db, Object tempids, Object tempid) {
        requireEvaApi();
        IFn resolve = Clojure.var("eva.api", "resolve-tempid");
        return resolve.invoke(db, tempids, tempid);
    }

    /**
     * Create a function which may be applied efficiently to a collection of tempids to resolve them.
     *
     * <p>
     * Use in place of applying {@link #resolveTempId(Database, Object, Object)} over a collection, which may be slow.
     * Intended for use with the results of a {@link eva.Connection#transact(List)} call.
     * </p>
     *
     * <pre>
     * {@code
     *  Long[] tempids = ...;
     *  Connection conn = Peer.connect(...);
     *  Map item1 = new HashMap() {{
     *      put(Keyword(":db/id", tempids[0]));
     *      ...;
     *  }};
     *  List tx = new ArrayList() {{
     *      add(item);
     *      ...;
     *  }};
     *  Map result = conn.transact(tx).get();
     *
     *  Function resolver = Peer.tempIdResolver(result.get(Connection.DB_AFTER),
     *                                          result.get(Connection.TEMPIDS));
     *
     *  Stream resolvedTempids = Arrays.stream(tempids).map(resolver);
     *  }
     *  </pre>
     *
     * @param db An eva database
     * @param tempids A map of tempids returned from a transaction
     * @return A function which, when passed a tempid, returns its resolved id
     */
    public static Function<Object,Object> tempIdResolver(Database db, Object tempids) {
        requireEvaApi();
        IFn resolve = Clojure.var("eva.api", "resolve-tempid");
        IFn resolverFn = (IFn)resolve.invoke(db, tempids);
        return (Object o) -> resolverFn.invoke(o);
    }

    /**
     * Return the partition associated with the specified entity id.
     *
     * @param entityId An entity id
     * @return The partition that entity lives in.
     */
    public static Object partition(Object entityId) {
        requireEvaApi();
        IFn part = Clojure.var("eva.api", "part");
        return part.invoke(entityId);
    }

    private static IFn squuidVar = Clojure.var("utiliva.uuid", "squuid");

    /**
     * Constructs a semi-sequential {@link java.util.UUID}.
     * Can be useful for having a unique identifier that does not fragment indexes.
     *
     * @return A semi-sequential uuid
     */
    public static UUID squuid() {
        return (UUID)squuidVar.invoke();
    }

    /** Returns the time component of a {@link #squuid()}, in the format of
     * {@link java.lang.System#currentTimeMillis()}.
     *
     * @param squuid A semi-sequential uuid as created by {@link #squuid()}
     * @return The time component of the squuid.
     */
    public static long squuidTimeMillis(UUID squuid) {
        IFn squuidTimeMillis = Clojure.var("utiliva.uuid", "squuid-time-millis");
        return (long)squuidTimeMillis.invoke(squuid);
    }

    /**
     * Generate a database function object.
     *
     * <pre>
     * {@code
     *  Map<Keyword, Object> f = new HashMap(){{
     *      put(Keyword.intern("lang"), "clojure");
     *      put(Keyword.intern("params"), Symbol.create("x"));
     *      put(Keyword.intern("code"), "(+ 1 x)");
     *  }};
     *
     *  IFn dbFunction = function(f);
     *  dbFunction.invoke(3); // 4
     * }
     * </pre>
     *
     * @param m A map of required function details -- the language, parameters, and code block.
     * @return A compiled, executable database function.
     */
    public static IFn function(Map m) {
        requireEvaApi();
        IFn function = Clojure.var("eva.api", "function");
        return (IFn)function.invoke(m);
    }

    /**
     * Construct a temporary id within the specified partition.
     * Tempids will be mapped to permanent ids within a single transaction.
     *
     * @param partition A clojure Keyword representing a partition to create a temporary id in
     * @return A temporary entity id
     */
    public static Object tempid(Object partition) {
        requireEvaApi();
        IFn tempid = Clojure.var("eva.api", "tempid");
        return tempid.invoke(partition);
    }

    /**
     * Construct a temporary id within the specified partition.
     * Tempids will be mapped to permanent ids within a single transaction.
     *
     * <p>User-created tempids are reserved for values of {@code n} within the range of -1 to -1000000 inclusive."

     * @param partition A clojure Keyword representing a partition to create a temporary id in
     * @param idNumber The number value to give to the tempid
     * @return A temporary entity id with the id number given
     */
    public static Object tempid(Object partition, long idNumber) {
        requireEvaApi();
        IFn tempid = Clojure.var("eva.api", "tempid");
        return tempid.invoke(partition, idNumber);
    }

    /**
     * Takes a {@code tx-num} or {@code tx-eid} and returns the equivalent {@code tx-num}.
     *
     * @param txNumOrTxEid A transaction number or eid
     * @return The equivalent transaction number
     */
    public static Long toTxNum(Long txNumOrTxEid) {
        requireEvaApi();
        IFn totxnum = Clojure.var("eva.api", "to-tx-num");
        return (Long) totxnum.invoke(txNumOrTxEid);
    }

    /**
     * Takes a {@code tx-num} or {@code tx-eid} and returns the equivalent {@code tx-eid}.
     *
     * @param txNumOrTxEid A transaction number or eid
     * @return The equivalent transaction eid
     */
    public static Long toTxEid(Long txNumOrTxEid) {
        requireEvaApi();
        IFn totxeid = Clojure.var("eva.api", "to-tx-eid");
        return (Long) totxeid.invoke(txNumOrTxEid);
    }

}
