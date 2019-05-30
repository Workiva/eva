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

package eva.error.v1;

import clojure.java.api.Clojure;
import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import clojure.lang.PersistentHashMap;

import recide.sanex.ISanitizable;
import recide.sanex.ISanitized;

import java.io.Serializable;
import java.util.UUID;
import java.util.Map;
import java.lang.StackTraceElement;

public class EvaException extends RuntimeException
        implements ISanitizable, ICodedExceptionInfo<EvaErrorCode>, Serializable {

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("recide.sanex"));
    }

    private static final long serialVersionUID = 100L;
    private static final IFn suppress = Clojure.var("recide.sanex", "apply-suppression");
    private static final IFn sanitize = Clojure.var("recide.sanex", "sanitize");
    private static final IPersistentMap EMPTY_DATA = (IPersistentMap) Clojure.read("{}");
    private static final Keyword kCause = Keyword.intern("cause");
    private static final Keyword kMessage = Keyword.intern("message");
    private static final Keyword kStack = Keyword.intern("stack-trace");
    private static final Keyword kType = Keyword.intern("type");
    private static final Keyword kData = Keyword.intern("data");
    private static final Keyword kRecursive = Keyword.intern("suppress-recursively?");
    private static Keyword errorTypeKey = Keyword.intern("eva", "error");
    private static Keyword errorCodeKey = Keyword.intern("eva.error", "code");

    private final String message;
    private EvaErrorCode errorCode;
    private final IPersistentMap data;
    private final UUID instanceID;

    public EvaException(EvaErrorCode errorCode, String message) {
        this(errorCode, message, EMPTY_DATA, null);
    }

    public EvaException(EvaErrorCode errorCode, String message, IPersistentMap data) {
        this(errorCode, message, data, null);
    }

    public EvaException(EvaErrorCode errorCode, String message, IPersistentMap data, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.data = (data != null) ? data : EMPTY_DATA;
        this.instanceID = null; // For future use when we cease sending exceptions directly over the wire.
        this.message = message;
    }

    /**
     * @deprecated Included only for v1 API.
     */
    @Deprecated
    public EvaException(EvaErrorCode errorCode, IPersistentMap data) {
        this(errorCode, "", data, null);
    }

    /**
     * @deprecated Included only for v1 API.
     */
    @Deprecated
    public EvaException(EvaErrorCode errorCode, IPersistentMap data, Throwable cause) {
        this(errorCode, "", data, cause);
    }

    /**
     * @deprecated Included only for v1 API.
     */
    @Deprecated
    public EvaException(String msg, IPersistentMap data) {
        this(EvaErrorCode.UNKNOWN_ERROR, msg, data, null);
    }

    /**
     * @deprecated Included only for v1 API.
     */
    @Deprecated
    public EvaException(String msg, IPersistentMap data, Throwable cause) {
        this(EvaErrorCode.UNKNOWN_ERROR, msg, data, cause);
    }

    public Keyword getErrorType() {
        return (Keyword) ((Map) this.getData()).get(this.errorTypeKey);
    }

    public boolean hasErrorType(Keyword k) {
        return getErrorType().equals(k);
    }

    public boolean hasErrorType(String k) {
        return hasErrorType(Keyword.intern(k));
    }

    public boolean hasErrorType(String ns, String name) {
        return hasErrorType(Keyword.intern(ns, name));
    }

    public UUID getInstanceId() {
        return instanceID;
    }

    @Override
    public EvaErrorCode getErrorCode() {
        return this.errorCode;
    }

    public void setErrorCode(EvaErrorCode code) {
        this.errorCode = code;
    }

    @Override
    public IPersistentMap getData() {
        IPersistentMap res = data;
        res = (!errorCode.isUnspecified() && !res.containsKey(errorTypeKey)) ? res.assoc(errorTypeKey, errorCode.getKey()) : res;
        res = (!errorCode.isUnspecified() && !res.containsKey(errorCodeKey)) ? res.assoc(errorCodeKey, errorCode.getCode()) : res;
        return res;
    }

    private String getExpandedMessage() {
        return String.format("%s[%s, %d]: %s (%s)",
                errorCode.getName(),
                errorCode.getScope(),
                errorCode.getCode(),
                errorCode.getExplanation(),
                message);
    }

    @Override
    public String toString() {
        String msg = getExpandedMessage();
        String sep = errorCode.isUnspecified() ? ": " : " ";
        String dataStr = (data != null) ? " " + data.toString() : "";
        return this.getClass().getSimpleName() + sep + msg + dataStr;
    }

    @Override
    public ISanitized getSanitized(IPersistentMap suppression) {
        IPersistentMap inputs =
                PersistentHashMap.create(kCause, this.getCause(),
                        kMessage, this.getMessage(),
                        kStack, this.getStackTrace(),
                        kType, this.getErrorType(),
                        kData, this.getData());

        Map values = (Map) suppress.invoke(inputs, suppression);
        Throwable cause = (Throwable) values.get(kCause);

        // suppress is a utility function that does not handle recursion,
        // specifically to enable class-specific implementations like this one.
        if ((boolean) ((Map) suppression).get(kRecursive)
                && cause != null) {
            if (cause instanceof EvaException) {
                cause = (Throwable) ((EvaException) cause).getSanitized(suppression);
            } else {
                cause = (Throwable) sanitize.invoke(cause, suppression);
            }
        }

        return new SanitizedEvaException(this,
                (IPersistentMap) values.get(kData),
                cause,
                (StackTraceElement[]) values.get(kStack),
                (String) values.get(kMessage));
    }
}
