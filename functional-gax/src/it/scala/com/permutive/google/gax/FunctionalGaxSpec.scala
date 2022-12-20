package com.permutive.google.gax

import cats.data.{Kleisli, NonEmptyMap, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._
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
  val family    = "d"

  override def tablesAndColumnFamilies: NonEmptyMap[String, NonEmptySet[String]] =
    NonEmptyMap.one("test-table", NonEmptySet.one(family))

  lazy val sut = FunctionalGax.impl[IO]

  test(
    "FunctionalGax.convertApiFuture should suspend the effect and not do anything until it is evaluated (interface)"
  ) {
    checkConvertSuspends(sut.convertApiFuture)
  }

  test(
    "FunctionalGax.convertApiFuture should suspend the effect and not do anything until it is evaluated (companion object)"
  ) {
    checkConvertSuspends(FunctionalGax.convertApiFuture)
  }

  def checkConvertSuspends(convertApiFuture: IO[ApiFuture[Void]] => IO[Void]): IO[Unit] = {
    val key       = "foo"
    val qualifier = "bar"
    val data      = "baz"

    for {
      // Insert row and check exists
      _ <- writeRowSync(key = key, qualifier = qualifier, data = data)
      _ <- assertIOBoolean(existsSync(key))
      // Creation deletion request, should not do anything
      deletion: IO[Unit] = convertApiFuture(deleteRowAsync(key)).void
      // Sleep and check row still there
      _ <- IO.sleep(1.second)
      _ <- assertIOBoolean(existsSync(key))
      // Evaluate IO and check row now deleted
      _ <- deletion
      _ <- assertIOBoolean(notExistsSync(key))
    } yield ()
  }

  test("FunctionalGax.convertServerStream should return the correct results (interface)") {
    checkServerStreamConverts(sut.convertServerStream(_, chunkSize = 128))
  }

  test("FunctionalGax.convertServerStream should return the correct results (companion object)") {
    checkServerStreamConverts(FunctionalGax.convertServerStream(_, chunkSize = 128))
  }

  def checkServerStreamConverts(convertServerStream: IO[ServerStream[Row]] => Stream[IO, Row]): IO[Unit] = {
    val key1 = "key-1"
    val key2 = "key-2"
    val key3 = "key-3"

    val qualifier = "foo"
    val data      = "bar"

    for {
      // Insert data and verify present
      _ <- writeRowSync(key = key1, qualifier = qualifier, data = data)
      _ <- writeRowSync(key = key2, qualifier = qualifier, data = data)
      _ <- writeRowSync(key = key3, qualifier = qualifier, data = data)
      _ <- assertIOBoolean(existsSync(key1))
      _ <- assertIOBoolean(existsSync(key2))
      _ <- assertIOBoolean(existsSync(key3))
      read = convertServerStream(IO(bigtableDataClient.readRows(Query.create(testTable))))
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

  test("FunctionalGax.convertBatcher batch as expected and flush on closing (interface)") {
    checkBatcherWorks(sut.convertBatcher)
  }

  test("FunctionalGax.convertBatcher batch as expected and flush on closing (companion object)") {
    checkBatcherWorks(FunctionalGax.convertBatcher)
  }

  def checkBatcherWorks(
    convertBatcher: IO[Batcher[RowMutationEntry, Void]] => Resource[IO, Kleisli[IO, RowMutationEntry, IO[Void]]]
  ): IO[Unit] = {
    val key1 = "key-1"
    val key2 = "key-2"
    val key3 = "key-3"
    val key4 = "key-3"

    val qualifier = "foo"
    val data      = "bar"

    convertBatcher(IO.delay(bigtableDataClient.newBulkMutationBatcher(testTable)))
      .use { batcher =>
        for {
          // Insert data and verify present
          _ <- writeRowSync(key = key1, qualifier = qualifier, data = data)
          _ <- writeRowSync(key = key2, qualifier = qualifier, data = data)
          _ <- writeRowSync(key = key3, qualifier = qualifier, data = data)
          _ <- writeRowSync(key = key4, qualifier = qualifier, data = data)
          _ <- assertIOBoolean(existsSync(key1))
          _ <- assertIOBoolean(existsSync(key2))
          _ <- assertIOBoolean(existsSync(key3))
          _ <- assertIOBoolean(existsSync(key4))
          // Delete synchronously (flattening) and check gone immediately
          _ <- deleteRowBatcher(key1, batcher).flatten
          _ <- assertIOBoolean(notExistsSync(key1))
          // Start a second deletion, check row still is there. This is a race so may be flaky!
          await <- deleteRowBatcher(key2, batcher)
          _     <- assertIOBoolean(existsSync(key2))
          // Start a third deletion, this _should_ batch with the first. Should also still exist immediately after
          await2 <- deleteRowBatcher(key3, batcher)
          _      <- assertIOBoolean(existsSync(key3))
          // Waiting for the third completion should mean both final rows are now gone
          _ <- await2
          _ <- assertIOBoolean(notExistsSync(key2))
          _ <- assertIOBoolean(notExistsSync(key3))
          // Await the second just for resource tidiness
          _ <- await
          // Start a 4th deletion but throw away the callback, check the resource closing waits for this to close!
          _ <- deleteRowBatcher(key4, batcher).void
        } yield ()
      }
      .flatMap(_ => assertIOBoolean(notExistsSync(key4))) // Check key 4 now gone
  }

  def deleteRowBatcher(key: String, batcher: Kleisli[IO, RowMutationEntry, IO[Void]]): IO[IO[Unit]] =
    IO(RowMutationEntry.create(key).deleteRow()).flatMap(batcher.run(_).map(_.void))

  def writeRowSync(key: String, qualifier: String, data: String): IO[Unit] =
    IO.delay(bigtableDataClient.mutateRow(writeMutation(key, qualifier, data)))

  def deleteRowAsync(key: String): IO[ApiFuture[Void]] =
    IO.delay(bigtableDataClient.mutateRowAsync(deleteRowMutation(key)))

  def existsSync(key: String): IO[Boolean] =
    IO.delay(bigtableDataClient.exists(testTable, key))

  def notExistsSync(key: String): IO[Boolean] =
    existsSync(key).map(!_)

  def writeMutation(key: String, qualifier: String, data: String): RowMutation =
    RowMutation.create(testTable, key, Mutation.create().setCell(family, qualifier, data))

  def deleteRowMutation(key: String): RowMutation =
    RowMutation.create(testTable, key, Mutation.create().deleteRow())

}
