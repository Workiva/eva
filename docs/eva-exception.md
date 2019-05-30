# EvaException

As of v1.0.0, *every* exception thrown by Eva should be an instance of [`EvaException`](/core/java-src/eva/error/v1/EvaException.java)<sup>1</sup>. `EvaException` extends [`RuntimeException`](https://docs.oracle.com/javase/8/docs/api/java/lang/RuntimeException.html). It implements Clojure's [`IExceptionInfo`](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IExceptionInfo.java), so it will have a map of data attached. It implements recide's [`ISanitizable`](https://github.com/Workiva/recide/blob/master/java-src/recide/sanex/ISanitizable.java), so you can acquire a ["sanitized"](https://github.com/Workiva/recide/blob/master/java-src/recide/sanex/ISanitized.java) version of the exception ([`SanitizedEvaException`](/core/java-src/eva/error/v1/SanitizedEvaException.java)) that *should* be safe to log (but may not be very useful).

## EvaErrorCode

Every EvaException is tagged with an "[error code.](/core/resources/eva/error/error_codes.edn)" These are intended to represent a hierarchy of error categories that users of Eva may care about. These may or may not correspond to the structure of the detailed error types used internally.

The intention is that as we move forward, our exception contract will stabilize around these Eva error codes. In the meantime, we are open to suggestions regarding structure or missing categories. In order to present a consistent set of error codes across the Eva ecosystem, we will likely spin the error code definitions out to a standalone repo from which client libraries can generate native representations.

Within Eva, the generated representation is the Java enum [`EvaErrorCode`](/core/java-src/eva/error/v1/EvaErrorCode.java). You can get this off an EvaException with [`getErrorCode()`](/core/java-src/eva/error/v1/EvaException.java#L112). You can use equality checks, but you can also incorporate the hierarchical element via the method [`is(EvaErrorCode eec)`](/core/java-src/eva/error/v1/EvaErrorCode.java#L501):

```java
EvaErrorCode eec = EvaErrorCode.INCORRECT_TRANSACT_SYNTAX;
// true, because they are identical:
eec.is(EvaErrorCode.INCORRECT_TRANSACT_SYNTAX);

// true, because INCORRECT_TRANSACT_SYNTAX is beneath INCORRECT_SYNTAX in the hierarchy:
eec.is(EvaErrorCode.INCORRECT_SYNTAX);

// similarly true:
eec.is(EvaErrorCode.API_ERROR);

// false:
eec.is(EvaErrorCode.ILLEGAL_TEMP_ID);
```

The EvaErrorCode has an associated numerical code (`getCode`), an associated http code for the Peer (`getHttpErrorCode`), and an explanation (`getExplanation`).

But if your only interaction with `EvaErrorCode` is to use `is`, there is a utility method available on EvaException:

```java
EvaException.getErrorCode().is(EvaErrorCode.API_ERROR);
// is equivalent to:
EvaException.hasErrorCode(EvaErrorCode.API_ERROR);
```

`hasErrorCode` delegates to `is`, so the error code hierarchy is respected.

## Error Types

Internally we tag all EvaExceptions with a Keyword representing its specific type. Because these error types are primarily intended for Eva's internal consumption, we do not make any promises regarding the stability of these error types. If you find that it is desirable to perform any logic on the basis of an internal Eva error type, it is probably desirable to make a support ticket asking for a corresponding `EvaErrorCode`. That said, our internal error types *are* exposed through several methods:

* `Keyword getErrorType()`
* `boolean hasErrorType(Keyword k)`
* `boolean hasErrorType(String k)`
* `boolean hasErrorType(String namespace, String name)`

```java
// All equivalent:
e.hasErrorType(Keyword.intern("transact-exception","nil-e");
e.hasErrorType("transact-exception/nil-e");
e.hasErrorType("transact-exception","nil-e");
```

## SanitizedEvaException

A sledgehammer solution to allow logging exceptions whose contents may or may not be sensitive, `SanitizedEvaException` is produced via [`recide.sanex/sanitize`](https://github.com/Workiva/recide/blob/master/src/recide/sanex.clj#L149). To retrieve the sanitized form of an EvaException, call its [`.getSanitized(IPersistentMap suppression)`](https://github.com/Workiva/eva-draft/blob/master/core/java-src/eva-draft/error/v1/EvaException.java#L139) method. This accepts a suppression map containing the following keyword options:

#### `:suppress-data?`

If this is true, all keywords will be stripped from ex-data except for the error-type keyword (:eva/error).

#### `:suppress-cause?`

If this is true, the cause of the exception will be removed. This is probably not what you want (see `:suppress-recursively?`).

#### `:suppress-message?`

If this is true, the message will be replaced with an empty string.

#### `:suppress-stack?`

If this is true, the stacktrace will be replaced with an empty stacktrace.

#### `:suppress-recursively?`

If this is true, the cause of the exception will be replaced by a sanitized cause, according to the same suppression levels.

#### `recide.sanex/*sanitization-level*`

A dynamic var that contains the current levels set in your environment (outside of dev profiles, probably the `default-sanitization`).

#### `recide.sanex/noop-sanitization`

This suppression map Explicitly sets every option to false so that no sanitization occurs.

#### `recide.sanex/default-sanitization`

This suppression map sets the defaults we use within Eva:

```clojure
;; DEFAULTS:
{:suppress-data? true,
 :suppress-cause? false,
 :suppress-message? true,
 :suppress-stack? false,
 :suppress-recursively? true}
```

### `recide.sanex.Utils`

This class contains a handful of static utility methods:

#### `getCurrentSanitizationLevel()`

Equivalent to deref'ing `recide.sanex/*sanitization-level*`.

#### `createSuppressionMap(...)`

Creates an IPersistentMap with the appropriate keywords corresponding to the boolean args.

#### `sanitize(Throwable)`, `sanitize(Throwable, IPersistentMap)`

Shortcut to Clojure IFn `recide.sanex/sanitize`.

**Footnotes**

1. This is not quite true -- if you [`invoke`](/core/java-src/eva/Database.java#L183) a database function, this is treated more or less as a direct invocation of the compiled code, and if the function throws any other type of exception, that exception will *not* be wrapped by `EvaException`.
