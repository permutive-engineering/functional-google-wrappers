/*
 * Copyright 2022 Permutive
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.permutive.testkit.munit.bigtable

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.google.cloud.bigtable.admin.v2.models.{CreateTableRequest, Table}
import com.google.cloud.bigtable.admin.v2.{
  BigtableTableAdminClient,
  BigtableTableAdminSettings
}
import com.google.cloud.bigtable.data.v2.{
  BigtableDataClient,
  BigtableDataSettings
}
import com.google.cloud.bigtable.emulator.v2.Emulator
import munit.CatsEffectSuite

import java.util.logging.Logger
import java.util.logging.Level

/** Test suite for integration testing against Bigtable using munit.
  *
  * Turns on an [[com.google.cloud.bigtable.emulator.v2.Emulator Emulator]] and
  * provides a
  * [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient]]
  * and
  * [[com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient BigtableTableAdminClient]].
  * In addition tables are created before all the tests run and data in tables
  * is cleared between each test.
  */
trait BigtableSuite extends CatsEffectSuite {

  /** Map of table name to column families to create.
    *
    * These are created before the tests start and any data present is cleared
    * between each individual test.
    */
  def tablesAndColumnFamilies: NonEmptyMap[String, NonEmptySet[String]]

  def projectId = "test-project"
  def instanceId = "test-instance"

  def bigtablePort(emulator: Emulator): Int = emulator.getPort
  def bigtableHost: String = "localhost"

  lazy val bigtableTableAdminClient: BigtableTableAdminClient =
    bigtableResourcesFixture()._1

  lazy val bigtableDataClient: BigtableDataClient =
    bigtableResourcesFixture()._2

  lazy val bigtableEmulator: Emulator =
    bigtableResourcesFixture()._3

  // Clear the data in all the tables between each test
  override def afterEach(context: AfterEach): Unit =
    clearAllTables(bigtableTableAdminClient).unsafeRunSync()

  // Set all BigTable log levels to WARNING to remove noise from tests results
  override def beforeAll(): Unit =
    List(
      classOf[Emulator],
      classOf[BigtableTableAdminSettings],
      classOf[BigtableDataSettings]
    )
      .map(_.getName())
      .map(Logger.getLogger(_))
      .foreach(_.setLevel(Level.WARNING))

  def createTableIfNotExists(
      client: BigtableTableAdminClient,
      tableName: String,
      columnFamilies: NonEmptySet[String]
  ): IO[Table] = IO {
    val req: CreateTableRequest = CreateTableRequest.of(tableName)
    columnFamilies.toSortedSet.foreach(req.addFamily)

    client.createTable(req)
  }

  def clearAllTables(client: BigtableTableAdminClient): IO[Unit] =
    tablesAndColumnFamilies.keys.toList.traverse_(clearTable(client, _))

  def clearTable(client: BigtableTableAdminClient, table: String): IO[Unit] =
    IO(client.dropAllRows(table)).void

  private lazy val bigtableResourcesFixture
      : Fixture[(BigtableTableAdminClient, BigtableDataClient, Emulator)] =
    ResourceSuiteLocalFixture("resources", resources)

  // End-users can add their own resources if they need to  and just append to this list using
  // `super.munitFixtures.append`
  override def munitFixtures: Seq[Fixture[_]] = List(bigtableResourcesFixture)

  private def resources
      : Resource[IO, (BigtableTableAdminClient, BigtableDataClient, Emulator)] =
    for {
      emulator <- emulatorResource
      adminClient <- tableAdminClientResource(emulator)
      dataClient <- dataClientResource(emulator)
      // Create the tables and column families as part of resource allocation
      _ <- Resource.eval(setupTables(adminClient))
    } yield (adminClient, dataClient, emulator)

  private def emulatorResource: Resource[IO, Emulator] =
    Resource.make(
      for {
        em <- IO(Emulator.createBundled())
        _ <- IO(em.start())
      } yield em
    )(emulator => IO(emulator.stop()))

  private def tableAdminClientResource(
      emulator: Emulator
  ): Resource[IO, BigtableTableAdminClient] =
    Resource.fromAutoCloseable(
      IO {
        val port = bigtablePort(emulator)

        val settingsBuilder =
          BigtableTableAdminSettings
            .newBuilderForEmulator(port)
            .setProjectId(projectId)
            .setInstanceId(instanceId)

        settingsBuilder
          .stubSettings()
          .setEndpoint(s"$bigtableHost:$port")

        BigtableTableAdminClient.create(settingsBuilder.build())
      }
    )

  private def dataClientResource(
      emulator: Emulator
  ): Resource[IO, BigtableDataClient] =
    Resource.fromAutoCloseable(
      IO.delay(
        BigtableDataClient.create(
          BigtableDataSettings
            .newBuilderForEmulator(bigtableHost, bigtablePort(emulator))
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build()
        )
      )
    )

  private def setupTables(client: BigtableTableAdminClient): IO[Unit] =
    tablesAndColumnFamilies.toSortedMap.toList.traverse_ {
      case (table, columnFamilies) =>
        createTableIfNotExists(client, table, columnFamilies)
    }

}
