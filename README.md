# Functional Google Wrapper Libraries

## Functional Gax

Converts some effects and classes in [Google API Extension (GAX)](https://github.com/googleapis/gax-java) and
[Google API Common] to functional ([Cats Effect] and [`fs2`]) equivalents.

Please read the [section below on binary compatibility](#versions-of-google-dependencies-and-binary-compatibility)!

### Dependency

```scala
"com.permutive" %% "functional-gax" % "<VERSION>"
```

### Functionality

All conversions are exposed via the [`FunctionalGax`] trait. A trait is used to allow consumers to use different
implementations in tests. Direct methods to perform conversions are available in the companion object if consumers wish
to avoid the interface.

Methods are available to:
 - Convert an [`ApiFuture`] to an arbitrary effect type, `F[_]`
   + This is part of Google API Common, not GAX
 - Convert a [`ServerStream`] to an [`fs2`] `Stream`
 - Convert a [`Batcher`] to a functional equivalent represented by a [`Kleisli`] in `F` of input to result
   + A [`Batcher`] can be used to lower I/O by batching requests together, this comes at the cost of extra latency

In addition an unsafe method to convert an effect to an [`ApiFuture`] is available in the companion object to
[`FunctionalGax`]. This is exposed as users may want this for testing.

### Versions of Google dependencies and binary compatibility

Please ensure that any code using this library includes integration tests!

The current version of GAX is set in [`Dependencies.scala`](project/Dependencies.scala). GAX does not obey any kind of
binary compatibility in relation to its version. For example `v1.63.0` introduced breaking changes compared to
`v1.62.0`. The diff is [here](https://github.com/googleapis/gax-java/compare/v1.62.0...v1.63.0).
As a result **the version of GAX used in this library will track the version used by the Google Java Bigtable library**,
[`google-cloud-bigtable`](https://github.com/googleapis/java-bigtable). Integration tests written against Bigtable
ensure that this library works with the Bigtable library.

Google API Core will be pulled in transiently by GAX; the version is not controlled here.

## Functional Google Cloud Bigtable

Wraps data interface in the [Google Bigtable Java library (`google-cloud-bigtable`)] to make methods referentially
transparent (cats-effect and fs2) and hide some Java wrinkles (e.g. `null` data). This still allows complete access to
the underlying Java API.

**Engineers are encouraged to use this library, rather than our older [`google-bigtable`] library, unless that library
meets exact requirements.** The full Bigtable Java interface is much more powerful than the one exposed by
[`google-bigtable`]. As a result using [`google-bigtable`] may lead applications to use Bigtable sub-optimally; or lead
to features being built into the application which are unnecessary.

### Dependencies

Core dependency:
```scala
"com.permutive" %% "functional-google-cloud-bigtable" % "<VERSION>"
```

[PureConfig] configuration:
```scala
"com.permutive" %% "functional-google-cloud-bigtable-pureconfig" % "<VERSION>"
```

### Functionality

Only the Java [`BigtableDataClient`] is wrapped and provided as [`FunctionalBigtableDataClient`]. This interface allows
retrieval and modification of cell data in Bigtable; it does not allow modification of the Bigtable instance itself, or
the tables it contains. This interface exposes all underlying functionality of the Java client, but not all methods. It
reduces the number of methods by:
- Not exposing blocking versions of methods, only non-blocking (and referentially transparent) methods exist
- Removing overloaded methods by using `Option`

See methods in the companion object of [`FunctionalBigtableDataClient`] to construct an instance. You will need to
either provide [`BigtableDataClientSettings`] to construct an underlying Java client, or the java client itself.

#### Constructing an underlying Java Bigtable client

See [`BigtableDataClientResource`] to construct an underlying Java client. This is not necessary if you provide
[`BigtableDataClientSettings`] to a construction method on [`FunctionalBigtableDataClient`] itself, but may be useful
for testing.

See the following section for instructions on loading [`BigtableDataClientSettings`] from configuration using
[PureConfig].

#### Loading from configuration (Pureconfig)

See [`BigtableDataClientConfig`] for configuration which can be loaded using [PureConfig]. This can be converted to the
core settings ([`EndpointSettings`]) by providing the extra arguments which are non-static (i.e. are functions).

## Authentication

In staging and production authentication should be provided by service accounts mounted to the running Kubernetes pod.
The underlying library will automatically pick this up and authenticate. This is standard practice at Permutive and
is managed in [`apps-stack`]. Applications using Golden Path >=2.0 will automatically mount these service accounts.

When using the Bigtable emulator (e.g. running locally, or testing) change the [`EndpointSettings`] (core library) or
[`EndpointConfig`] (pureconfig module) to indicate the target instance is an emulator. This removes the need for
authentication.

### Using the underlying library

For help with, and examples of using, Bigtable and the underlying Bigtable Java client see:
- [The Google Java Bigtable library itself](https://github.com/googleapis/java-bigtable)
- [Code samples](https://github.com/googleapis/java-bigtable/tree/master/samples/snippets/src/main/java/com/example/bigtable) from the Google Java Library ([permalink](https://github.com/googleapis/java-bigtable/tree/11da7ce5d05557cfedff354ac23e2a4ae4c40bbc/samples/snippets/src/main/java/com/example/bigtable))
- [API docs for the Java Bigtable library](https://googleapis.dev/java/google-cloud-bigtable/latest/index.html) (These are hard to follow, browsing the code in an IDE is probably easier)
- Google documentation on:
  + [Client library](https://cloud.google.com/bigtable/docs/reference/libraries)
  + [Low-levels RPCs in Bigtable itself](https://cloud.google.com/bigtable/docs/reference/data/rpc)
    * These usually have an exact analogue in the underlying library
- [Tests in this repository](functional-google-cloud-bigtable/src/test/scala/com/permutive/google/bigtable/data/FunctionalBigtableDataClientSpec.scala)

### Testing

If you write tests with [MUnit] then [`testkit-munit-bigtable`] can help you write integration tests against Bigtable. It
includes a helper which stands up an emulator and provisions tables for you. See examples in that repo or
[examples in this repo](functional-google-cloud-bigtable/src/test/scala/com/permutive/google/bigtable/data/FunctionalBigtableDataClientSpec.scala)
of its use.

[`FunctionalBigtableDataClient`]: functional-google-cloud-bigtable/src/main/scala/com/permutive/google/bigtable/data/FunctionalBigtableDataClient.scala
[`BigtableDataClientSettings`]: functional-google-cloud-bigtable/src/main/scala/com/permutive/google/bigtable/data/BigtableDataClientSettings.scala
[`EndpointSettings`]: functional-google-cloud-bigtable/src/main/scala/com/permutive/google/bigtable/data/BigtableDataClientSettings.scala
[`BigtableDataClientResource`]: functional-google-cloud-bigtable/src/main/scala/com/permutive/google/bigtable/data/BigtableDataClientResource.scala

[`BigtableDataClientConfig`]: functional-google-cloud-bigtable-pureconfig/src/main/scala/com/permutive/google/bigtable/data/BigtableDataClientConfig.scala
[`EndpointConfig`]: functional-google-cloud-bigtable-pureconfig/src/main/scala/com/permutive/google/bigtable/data/BigtableDataClientConfig.scala

[`testkit-munit-bigtable`]: testkit-munit-bigtable

[Google Bigtable Java library (`google-cloud-bigtable`)]: https://github.com/googleapis/java-bigtable
[`BigtableDataClient`]: https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/BigtableDataClient.html

[MUnit]: https://scalameta.org/munit/
[PureConfig]: https://github.com/pureconfig/pureconfig

[`functional-gax`]: #functional-gax

[Google API Common]: https://github.com/googleapis/api-common-java
[Cats Effect]: https://github.com/typelevel/cats-effect
[`fs2`]: https://github.com/typelevel/fs2

[`ApiFuture`]: https://javadoc.io/doc/com.google.api/api-common/1.10.1/com/google/api/core/ApiFuture.html
[`Batcher`]: https://javadoc.io/doc/com.google.api/gax/1.62.0/com/google/api/gax/batching/Batcher.html
[`ServerStream`]: https://javadoc.io/doc/com.google.api/gax/1.62.0/com/google/api/gax/rpc/ServerStream.html

[`Kleisli`]: https://typelevel.org/cats/datatypes/kleisli.html

[`FunctionalGax`]: functional-gax/src/main/scala/com/permutive/google/gax/FunctionalGax.scala
