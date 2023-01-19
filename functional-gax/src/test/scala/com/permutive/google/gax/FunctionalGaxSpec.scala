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

package com.permutive.google.gax

import cats.data.{Kleisli, NonEmptyMap, NonEmptySet}
import cats.effect.{IO, Resource}
import com.google.api.core.ApiFuture
import com.google.api.gax.batching.Batcher
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models._
import com.permutive.testkit.munit.bigtable.BigtableSuite
import fs2.Stream

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class FunctionalGaxSpec extends BigtableSuite {

  val testTable = "test-table"
  val family = "d"

  override def tablesAndColumnFamilies: NonEmptyMap[String, NonEmptySet[String]] =
    NonEmptyMap.one("test-table", NonEmptySet.one(family))

  test(
    "FunctionalGax.convertApiFuture should suspend the effect and not do anything until it is evaluated"
  ) {
    checkConvertSuspends(FunctionalGax.convertApiFuture)
  }

  def checkConvertSuspends(
      convertApiFuture: IO[ApiFuture[Void]] => IO[Void]
  ): IO[Unit] = {
    val key = "foo"
    val qualifier = "bar"
    val data = "baz"

    for {
      // Insert row and check exists
      _ <- writeRowSync(key = key, qualifier = qualifier, data = data)
      _ <- existsSync(key).assert
      // Creation deletion request, should not do anything
      deletion: IO[Unit] = convertApiFuture(deleteRowAsync(key)).void
      // Sleep and check row still there
      _ <- IO.sleep(1.second)
      _ <- existsSync(key).assert
      // Evaluate IO and check row now deleted
      _ <- deletion
      _ <- notExistsSync(key).assert
    } yield ()
  }

  test(
    "FunctionalGax.convertServerStream should return the correct results"
  ) {
    checkServerStreamConverts(
      FunctionalGax.convertServerStream(_, chunkSize = 128)
    )
  }

  def checkServerStreamConverts(
      convertServerStream: IO[ServerStream[Row]] => Stream[IO, Row]
  ): IO[Unit] = {
    val key1 = "key-1"
    val key2 = "key-2"
    val key3 = "key-3"

    val qualifier = "foo"
    val data = "bar"

    for {
      // Insert data and verify present
      _ <- writeRowSync(key = key1, qualifier = qualifier, data = data)
      _ <- writeRowSync(key = key2, qualifier = qualifier, data = data)
      _ <- writeRowSync(key = key3, qualifier = qualifier, data = data)
      _ <- existsSync(key1).assert
      _ <- existsSync(key2).assert
      _ <- existsSync(key3).assert
      read = convertServerStream(
        IO(bigtableDataClient.readRows(Query.create(testTable)))
      )
      res <- read.compile.toList
    } yield {
      assertEquals(res.size, 3, res)

      val keys = res.map(_.getKey.toStringUtf8).toSet
      assertEquals(keys, Set(key1, key2, key3))

      val cells = res.flatMap(_.getCells.asScala.toList)
      assertEquals(cells.size, 3, cells)

      val qualifiers = cells.map(_.getQualifier.toStringUtf8).toSet
      assertEquals(qualifiers, Set(qualifier))

      val cellData = cells.map(_.getValue.toStringUtf8).toSet
      assertEquals(cellData, Set(data))
    }
  }

  // This was mostly excercised in unit tests already, these are just integration tests

  test(
    "FunctionalGax.convertBatcher batch as expected and flush on closing"
  ) {
    checkBatcherWorks(FunctionalGax.convertBatcher)
  }

  def checkBatcherWorks(
      convertBatcher: IO[Batcher[RowMutationEntry, Void]] => Resource[
        IO,
        Kleisli[IO, RowMutationEntry, IO[Void]]
      ]
  ): IO[Unit] = {
    val key1 = "key-1"
    val key2 = "key-2"
    val key3 = "key-3"
    val key4 = "key-3"

    val qualifier = "foo"
    val data = "bar"

    convertBatcher(
      IO.delay(bigtableDataClient.newBulkMutationBatcher(testTable))
    ).use { batcher =>
      for {
        // Insert data and verify present
        _ <- writeRowSync(key = key1, qualifier = qualifier, data = data)
        _ <- writeRowSync(key = key2, qualifier = qualifier, data = data)
        _ <- writeRowSync(key = key3, qualifier = qualifier, data = data)
        _ <- writeRowSync(key = key4, qualifier = qualifier, data = data)
        _ <- existsSync(key1).assert
        _ <- existsSync(key2).assert
        _ <- existsSync(key3).assert
        _ <- existsSync(key4).assert
        // Delete synchronously (flattening) and check gone immediately
        _ <- deleteRowBatcher(key1, batcher).flatten
        _ <- notExistsSync(key1).assert
        // Start a second deletion, check row still is there. This is a race so may be flaky!
        await <- deleteRowBatcher(key2, batcher)
        _ <- existsSync(key2).assert
        // Start a third deletion, this _should_ batch with the first. Should also still exist immediately after
        await2 <- deleteRowBatcher(key3, batcher)
        _ <- existsSync(key3).assert
        // Waiting for the third completion should mean both final rows are now gone
        _ <- await2
        _ <- notExistsSync(key2).assert
        _ <- notExistsSync(key3).assert
        // Await the second just for resource tidiness
        _ <- await
        // Start a 4th deletion but throw away the callback, check the resource closing waits for this to close!
        _ <- deleteRowBatcher(key4, batcher).void
      } yield ()
    }
      .flatMap(_ => notExistsSync(key4).assert) // Check key 4 now gone
  }

  def deleteRowBatcher(
      key: String,
      batcher: Kleisli[IO, RowMutationEntry, IO[Void]]
  ): IO[IO[Unit]] =
    IO(RowMutationEntry.create(key).deleteRow())
      .flatMap(batcher.run(_).map(_.void))

  def writeRowSync(key: String, qualifier: String, data: String): IO[Unit] =
    IO.delay(bigtableDataClient.mutateRow(writeMutation(key, qualifier, data)))

  def deleteRowAsync(key: String): IO[ApiFuture[Void]] =
    IO.delay(bigtableDataClient.mutateRowAsync(deleteRowMutation(key)))

  def existsSync(key: String): IO[Boolean] =
    IO.delay(bigtableDataClient.exists(testTable, key))

  def notExistsSync(key: String): IO[Boolean] =
    existsSync(key).map(!_)

  def writeMutation(key: String, qualifier: String, data: String): RowMutation =
    RowMutation.create(
      testTable,
      key,
      Mutation.create().setCell(family, qualifier, data)
    )

  def deleteRowMutation(key: String): RowMutation =
    RowMutation.create(testTable, key, Mutation.create().deleteRow())

}
