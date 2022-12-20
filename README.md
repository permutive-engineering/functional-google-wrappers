
# Functional Gax

Converts some effects and classes in [Google API Extension (GAX)](https://github.com/googleapis/gax-java) and
[Google API Common] to functional ([Cats Effect] and [`fs2`]) equivalents.

Please read the [section below on binary compatibility](#versions-of-google-dependencies-and-binary-compatibility)!

## Dependency

```scala
"com.permutive" %% "functional-gax" % "<VERSION>"
```

## Functionality

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

## Versions of Google dependencies and binary compatibility

Please ensure that any code using this library includes integration tests!

The current version of GAX is set in [`Dependencies.scala`](project/Dependencies.scala). GAX does not obey any kind of
binary compatibility in relation to its version. For example `v1.63.0` introduced breaking changes compared to
`v1.62.0`. The diff is [here](https://github.com/googleapis/gax-java/compare/v1.62.0...v1.63.0).
As a result **the version of GAX used in this library will track the version used by the Google Java Bigtable library**,
[`google-cloud-bigtable`](https://github.com/googleapis/java-bigtable). Integration tests written against Bigtable
ensure that this library works with the Bigtable library.

Google API Core will be pulled in transiently by GAX; the version is not controlled here.

[Google API Common]: https://github.com/googleapis/api-common-java
[Cats Effect]: https://github.com/typelevel/cats-effect
[`fs2`]: https://github.com/typelevel/fs2

[`ApiFuture`]: https://javadoc.io/doc/com.google.api/api-common/1.10.1/com/google/api/core/ApiFuture.html
[`Batcher`]: https://javadoc.io/doc/com.google.api/gax/1.62.0/com/google/api/gax/batching/Batcher.html
[`ServerStream`]: https://javadoc.io/doc/com.google.api/gax/1.62.0/com/google/api/gax/rpc/ServerStream.html

[`Kleisli`]: https://typelevel.org/cats/datatypes/kleisli.html

[`FunctionalGax`]: functional-gax/src/main/scala/com/permutive/google/gax/FunctionalGax.scala
