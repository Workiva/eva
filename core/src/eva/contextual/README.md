## Contextual API

The `eva.contextual` API embellishes metrics, tracing and logging with
lexical and runtime contexts. Runtime context captures bits of relevant information
available during function **execution time** and it is just a hashmap of
tags. Lexical context captures information about **where in the code** something happens
and consists of most important `FnForm` fields such as
namespace, function name and function parameters. See usage examples
[here](../../../test/eva/contextual/usage_examples.clj).

The API has two parts:

  - aspects providing way to customize how new runtime information should be merged with already accumulated one
  - context aware helpers which could be used inside functions

There are four aspects provided:

  - `eva.contextual.core/capture` - merges new runtime information with already existing one and maintains stack of contexts isomoprphic to function call stack
  - `eva.contextual.core/timed` - creates timer to report function execution time.
  - `eva.contextual.core/logged` - logs when function gets called (entrance and potentially exit).
  - `eva.contextual.core/traced` - trace function call.

There is maintained stack of contexts and `capture` adds new entry to the stack:

  - if argument is a hashmap then it gets merged with existing runtime
  - if argument is a vector then new context get created with tags listed in vector
  provided they already exist in topmost context


Context captured on high level (API) could be later used for reporting on lower
levels where details available on high level are not available.

The `timed`, `logged` and `traced` aspects do not create new entry in context stack, however
they allow to specify tags:

```
(morphe.core/defn ^{::d/aspects [(c/timed [:a])]})
```

The `timed` aspect in code above would try to create timer with tag `:a` provided it
does exist in current context. It would not fail if such tag do not exist, i.e. `:a`
would be just ignored in that case.




The reason beyond having `capture` aspect which updates contexts stack
and other aspects which do not modify contexts stack is:

  _We do not know in advance on
high level what kind of context would be needed on lower level.
However interesting things could get captured "just in
case". Aspects dealing with any sort of reporting (logging, tracing,
metrics) are free to choose what exactly needs to be reported. For
example logging could always enrich logged entries with runtime tags,
but timer might decide not to do this due cost of having many tags on
metrics API provider. This is why decision about what has to be reported
is made "per aspect" rather than "per function"._

Each of `timed`, `traced`, `logged` aspects accept either vector for
finding existing context or a hashmap for merging with existing
context.  If no arguments provided it means an "no runtime for aspect",
i.e. captured context just ignored.
