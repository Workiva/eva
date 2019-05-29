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
import eva.error.v1.EvaErrorCode;
import eva.error.v1.EvaException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A connection to a database, which can be used to submit transactions and retrieve the value of the database.
 *
 * <p>Connections are cached, thread-safe, and intended to be long-lived.
 */
public interface Connection {
    Object DB_AFTER = Clojure.read(":db-after");
    Object DB_BEFORE = Clojure.read(":db-before");
    Object TEMPIDS = Clojure.read(":tempids");
    Object TX_DATA = Clojure.read(":tx-data");

    /**
     * Yields the most recently known database snapshot, which is maintained in
     * memory.
     *
     * @return immutable database value
     */
    Database dbSnapshot();

    /**
     * Same as 'dbSnapshot'.
     *
     * @return immutable database value
     */
    Database db();

    /**
     * Like 'dbSnapshot', but forces reads from the backing store to assert whether or
     * not the in-memory database snapshot is stale before returning. If the
     * snapshot is found to be stale, this call will block until the updated
     * snapshot is produced from storage.
     *
     * <p>Communicates only with storage.
     *
     * <p>Intended for use when stale state on a Peer is suspected.
     *
     * @return immutable database value
     */
    Database syncDb();

    /**
     * Provides the database value at transaction t.
     *
     * @param t transaction id
     * @return immutable database value at transaction t
     */
    Database dbAt(Object t);

    /**
     * Releases the resources associated with this connection.
     *
     * <p>Connections are intended to be long-lived, so you should release
     * a connection only when the entire program is finished with it
     * (for example at program shutdown).
     */
    void release();

    /**
     * Returns the current value of the transaction log.
     * Communicates with storage, but not with the transactor
     *
     * @return immutable transaction log
     */
    Log log();

    /**
     * Returns the latest tx-num that the Connection has updated its local state
     * to match.
     *
     * @return the latest known tx-num on this connection
     */
    Long latestT();

    /**
     * Submits a transaction, blocking until a result is available.
     *
     * @param txData list of operations to be processed, including assertions, retractions, functions, or entity-maps
     * @return a CompletableFuture which can monitor the status of the transaction. On successful commit, the future
     *         will contain a map of the following keys:
     *         <dl>
     *         <dt>{@link #DB_BEFORE}</dt> <dd>database value before transaction was applied</dd>
     *         <dt>{@link #DB_AFTER}</dt> <dd>database value after transaction was applied</dd>
     *         <dt>{@link #TX_DATA}</dt> <dd>collection of primitive operations performed by the transaction</dd>
     *         <dt>{@link #TEMPIDS}</dt> <dd>can be used with {@link Peer#resolveTempId(Database, Object, Object)}
     *         to resolve temporary ids  used in the txData</dd>
     *         </dl>
     *
     *         If the transaction fails or timed out, attempts to {@link CompletableFuture#get()} the future's value will raise an {@link ExecutionException}.
     *         When getting the result of the future, catch {@link ExecutionException} and call {@link ExecutionException#getCause()}
     *         to retrieve the underlying error.
     *
     * @exception EvaException raised if the internal state of the Connection is in a bad state. See {@link EvaErrorCode}
     *            for possible error causes.
     *
     */
    CompletableFuture<java.util.Map> transact(java.util.List txData);

    /**
     * Like {@link #transact(List)} but returns immediately without waiting for the transaction to complete.
     *
     * @param txData see {@link #transact(List)}
     * @return see {@link #transact(List)}
     * @exception EvaException raised if the internal state of the Connection is in a bad state. See {@link EvaErrorCode}
     *            for possible error cases.
     */
    CompletableFuture<java.util.Map> transactAsync(java.util.List txData);

    /**
     * Get the status of this Connection to eva.
     */
    Map<Object,Object> status();
}
