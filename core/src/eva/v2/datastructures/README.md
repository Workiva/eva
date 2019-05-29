# A note on upgrading datastructures

Our datastructures are an important part of EVA, and we would like continually to be making improvements to the underlying datastructure. To that end, this package allows for seamless, lazy datastructure upgrades to existing EVA instances. The design is generic, allowing implementations to pursue whatever kind of upgrade strategy implementers prefer.

There are only two high-level pieces of this:

* A [single protocol, `Versioned`](/core/src/eva/datastructures/protocols.clj#L34), containing a single method, `get-version`. Each datastructure is responsible for implementing its own versioning path. Currently, only our B-ε tree provides a full implementation.
* An [`ensure-version` function](/core/src/eva/datastructures/versioning.clj#L25), implemented through the [multimethod `version-conversion`](/core/src/eva/datastructures/versioning.clj#L5), which is the entrypoint to converting between versions of a datastructure.

Currently only the B<sup>ε</sup>-tree code has been restructured for ease of using this functionality. An example of how versioning might be implemented can be seen in these test files:

* [Redirecting api.clj implementations to the new datastructure version](/dev/src/eva/datastructures/test_version/api.clj)
* [Ensuring that fressian readers and writers use distinct tags](/dev/src/eva/datastructures/test_version/fressian.clj)
* [Providing implementations of multimethod `version-conversion`](/dev/src/eva/datastructures/test_version/logic/versioning.clj#L89)
* [A missing piece of the last file, separated for testing purposes](/dev/src/eva/datastructures/test_version/logic/missing_versioning.clj)
* [Actually calling `ensure-version` after reading from storage](/dev/src/eva/datastructures/test_version/logic/nodes.clj#L196)

An alternate method of implementation would be to re-use our message buffers to insert upgrade messages, relying on current propagation patterns for the upgrade messages to work their way through the tree. However, while we may use this in the future, it would put a number of limitations on the types and extent of changes we could make, forcing the conversion of each node to work without any context derived from hierarchical context existing above that node in the B<sup>ε</sup>-tree.
