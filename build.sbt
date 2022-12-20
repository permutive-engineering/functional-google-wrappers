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

val CatsEffect = "3.4.2"

val Cats = "2.9.0"

val Munit = "0.7.29"

val MunitCE3 = "1.0.7"

val ScalacheckEffect = "1.0.4"

val Scala213 = "2.13.10"
ThisBuild / crossScalaVersions := Seq("2.13.14", Scala213, "3.1.2")
ThisBuild / scalaVersion := Scala213 // the default Scala

lazy val root = tlCrossRootProject.aggregate(functionalGax)

lazy val functionalGax = crossProject(JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("functional-gax"))
  .settings(
    name := "functional-gax",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-core" % Cats,
      "org.typelevel" %%% "cats-effect-std" % CatsEffect,
      "co.fs2" %%% "fs2-core" % "3.2.14",
      "com.google.api" % "gax" % "2.20.1",
      "org.typelevel" %%% "cats-effect" % CatsEffect % "test,it",
      "org.typelevel" %% "cats-effect-testkit" % CatsEffect % Test,
      "org.scalameta" %%% "munit" % Munit % Test,
      "org.typelevel" %%% "munit-cats-effect-3" % MunitCE3 % Test,
      "org.typelevel" %%% "scalacheck-effect-munit" % ScalacheckEffect % Test,
      "com.google.cloud" % "google-cloud-bigtable" % "2.17.1" % IntegrationTest,
      "com.google.cloud" % "google-cloud-bigtable-emulator" % "0.154.1" % IntegrationTest
    )
  )
  .configs(IntegrationTest)
