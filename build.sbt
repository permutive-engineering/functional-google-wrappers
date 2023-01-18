// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.0" // your current series x.y

ThisBuild / organization := "com.permutive"
ThisBuild / organizationName := "Permutive"
ThisBuild / startYear := Some(2022)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("janstenpickle", "Chris Jansen")
)

// publish to s01.oss.sonatype.org (set to true to publish to oss.sonatype.org instead)
ThisBuild / tlSonatypeUseLegacyHost := true

val Bigtable = "2.17.1"

val CatsEffect = "3.4.2"

val Cats = "2.9.0"

val Munit = "0.7.29"

val MunitCE3 = "1.0.7"

val ScalacheckEffect = "1.0.4"

val Scala213 = "2.13.10"
ThisBuild / crossScalaVersions := Seq("2.12.14", Scala213, "3.2.1")
ThisBuild / scalaVersion := Scala213 // the default Scala

lazy val root =
  tlCrossRootProject
    .aggregate(
      functionalGax,
      functionalGoogleCloudBigtable,
      testkitMunitBigtable
    )

lazy val functionalGax = project
  .in(file("functional-gax"))
  .settings(
    name := "functional-gax",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % Cats,
      "org.typelevel" %%% "cats-effect-std" % CatsEffect,
      "co.fs2" %%% "fs2-core" % "3.2.14",
      "com.google.api" % "gax" % "2.20.1",
      "org.typelevel" %%% "cats-effect" % CatsEffect % Test,
      "org.typelevel" %%% "cats-effect-testkit" % CatsEffect % Test,
      "org.scalameta" %%% "munit" % Munit % Test,
      "org.typelevel" %%% "munit-cats-effect-3" % MunitCE3 % Test,
      "org.typelevel" %%% "scalacheck-effect-munit" % ScalacheckEffect % Test
    ),
    libraryDependencies ++= PartialFunction
      .condOpt(CrossVersion.partialVersion(scalaVersion.value)) {
        case Some((2, 12)) =>
          "org.scala-lang.modules" %%% "scala-collection-compat" % "2.8.1"
      }
      .toList
  )
  .dependsOn(testkitMunitBigtable % "test->compile")

lazy val functionalGoogleCloudBigtable = project
  .in(file("functional-google-cloud-bigtable"))
  .settings(
    name := "functional-google-cloud-bigtable",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % Cats,
      "org.typelevel" %%% "cats-effect-std" % CatsEffect,
      "com.google.cloud" % "google-cloud-bigtable" % Bigtable,
      "com.comcast" %%% "ip4s-core" % "3.2.0",
      "com.permutive" %%% "gcp-types" % "1.0.0-RC1",
      "org.typelevel" %%% "cats-effect" % CatsEffect % Test,
      "org.typelevel" %%% "cats-effect-testkit" % CatsEffect % Test,
      "org.scalameta" %%% "munit" % Munit % Test,
      "org.typelevel" %%% "munit-cats-effect-3" % MunitCE3 % Test,
      "org.typelevel" %%% "scalacheck-effect-munit" % ScalacheckEffect % Test
    ),
    libraryDependencies ++= PartialFunction
      .condOpt(CrossVersion.partialVersion(scalaVersion.value)) {
        case Some((2, 12)) =>
          "org.scala-lang.modules" %%% "scala-collection-compat" % "2.8.1" % Test
      }
      .toList
  )
  .dependsOn(functionalGax, testkitMunitBigtable % "test->compile")

lazy val testkitMunitBigtable = project
  .in(file("testkit-munit-bigtable"))
  .settings(
    name := "testkit-munit-bigtable",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % Cats,
      "org.typelevel" %%% "cats-effect" % CatsEffect,
      "org.typelevel" %% "cats-effect-testkit" % CatsEffect,
      "org.scalameta" %%% "munit" % Munit,
      "org.typelevel" %%% "munit-cats-effect-3" % MunitCE3,
      "com.google.cloud" % "google-cloud-bigtable" % Bigtable,
      "com.google.cloud" % "google-cloud-bigtable-emulator" % "0.154.1"
    ),
    libraryDependencies ++= PartialFunction
      .condOpt(CrossVersion.partialVersion(scalaVersion.value)) {
        case Some((2, 12)) =>
          "org.scala-lang.modules" %%% "scala-collection-compat" % "2.8.1" % Test
      }
      .toList
  )
