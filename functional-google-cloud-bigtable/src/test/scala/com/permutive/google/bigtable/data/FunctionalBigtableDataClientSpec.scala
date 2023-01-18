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

package com.permutive.google.bigtable.data

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.IO
import cats.effect.kernel.Resource
import com.google.cloud.bigtable.data.v2.models.Filters.FILTERS
import com.google.cloud.bigtable.data.v2.models._
import com.google.common.primitives.Longs
import com.google.protobuf.ByteString
import com.permutive.google.gcp.types.ProjectId
import com.permutive.testkit.munit.bigtable.BigtableSuite
import com.comcast.ip4s._
import fs2.Chunk

import scala.jdk.CollectionConverters._

class FunctionalBigtableDataClientSpec extends BigtableSuite {

  val emptyStringTable = ""
  val testTable = "test-table"

  val familyA = "a"
  val familyB = "b"

  val qualifierA = "a"

  val key = "key"
  val keyByteString = ByteString.copyFromUtf8(key)

  val emptyKey = ""
  val emptyKeyByteString = ByteString.copyFromUtf8(emptyKey)

  lazy val config = BigtableDataClientSettings[IO](
    projectId = ProjectId.fromString(projectId).get,
    instanceId = instanceId
  ).withEndpoint(
    EndpointSettings.emulator(
      bigtableHost,
      Port.fromInt(bigtablePort(bigtableEmulator)).get
    )
  )

  lazy val sutResource: Resource[IO, FunctionalBigtableDataClient[IO]] =
    FunctionalBigtableDataClient.resource(config)

  override def tablesAndColumnFamilies
      : NonEmptyMap[String, NonEmptySet[String]] =
    NonEmptyMap.of(
      testTable -> NonEmptySet.of(familyA, familyB),
      emptyStringTable -> NonEmptySet.of(familyA, familyB)
    )

  test(
    "FunctionalBigtableDataClient.exists(String) should return false if the row does not exist"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(testTable, key))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.exists(String) should return true if the row exists"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut =>
        for {
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- sut.exists(testTable, key)
        } yield res
      )

    assertIO(program, true)
  }

  test(
    "FunctionalBigtableDataClient.exists(String) should not raise an error if the table is an empty string"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(emptyStringTable, key))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.exists(String) should not raise an error if the key is an empty string"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(testTable, emptyKey))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.exists(ByteString) should return false if the row does not exist"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(testTable, keyByteString))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.exists(ByteString) should return true if the row exists"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut =>
        for {
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- sut.exists(testTable, key)
        } yield res
      )

    assertIO(program, true)
  }

  test(
    "FunctionalBigtableDataClient.exists(ByteString) not raise an error if the table is an empty string"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(emptyStringTable, keyByteString))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.exists(ByteString) not raise an error if the key is an empty string"
  ) {
    val program: IO[Boolean] =
      sutResource.use(sut => sut.exists(testTable, emptyKeyByteString))

    assertIO(program, false)
  }

  test(
    "FunctionalBigtableDataClient.readRow(String) should return None if the row does not exist"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(testTable, key, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(String) should return None if the row exists but is filtered out"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut =>
        for {
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- sut.readRow(
            testTable,
            key,
            Some(FILTERS.key().exactMatch("DO-NOT-MATCH"))
          )
        } yield res
      )

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(String) should return the Row if it exists"
  ) {
    sutResource.use(sut =>
      for {
        _ <- IO.delay(
          bigtableDataClient.mutateRow(
            RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
          )
        )
        res <- sut.readRow(testTable, key, None)
      } yield {
        assert(clue(res).isDefined, "Row was not found")
        assertEquals(res.get.getKey.toStringUtf8, key)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.readRow(String) should return None if the table is an empty string"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(emptyStringTable, key, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(String) should return None if the key is an empty string"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(testTable, emptyKey, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(ByteString) should return None if the row does not exist"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(testTable, keyByteString, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(ByteString) should return None if the row exists but is filtered out"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut =>
        for {
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- sut.readRow(
            testTable,
            keyByteString,
            Some(FILTERS.key().exactMatch("DO-NOT-MATCH"))
          )
        } yield res
      )

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(ByteString) should return the Row if it exists"
  ) {
    sutResource.use(sut =>
      for {
        _ <- IO.delay(
          bigtableDataClient.mutateRow(
            RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
          )
        )
        res <- sut.readRow(testTable, keyByteString, None)
      } yield {
        assert(clue(res).isDefined, "Row was not found")
        assertEquals(res.get.getKey.toStringUtf8, key)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.readRow(ByteString) should return None if the table is an empty string"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(emptyStringTable, keyByteString, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRow(ByteString) should return None if the key is an empty string"
  ) {
    val program: IO[Option[Row]] =
      sutResource.use(sut => sut.readRow(testTable, emptyKeyByteString, None))

    assertIO(program, None)
  }

  test(
    "FunctionalBigtableDataClient.readRows should return no rows if none exist"
  ) {
    val program: IO[List[Row]] =
      sutResource.use(
        _.readRows(
          Query.create(testTable).rowKey(key).rowKey("another key"),
          streamChunkSize = 128
        ).compile.toList
      )

    assertIO(program, List.empty)
  }

  test(
    "FunctionalBigtableDataClient.readRows should return rows which exists"
  ) {
    sutResource.use(sut =>
      for {
        _ <- IO.delay(
          bigtableDataClient.mutateRow(
            RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
          )
        )
        res <- sut
          .readRows(
            Query.create(testTable).rowKey(key).rowKey("another key"),
            streamChunkSize = 128
          )
          .compile
          .toList
      } yield {
        assertEquals(clue(res).size, 1, "Incorrect number of rows returned")
        assertEquals(res.head.getKey.toStringUtf8, key)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.sampleRowKeys should return no rows if none exist"
  ) {
    val program: IO[Chunk[KeyOffset]] =
      sutResource.use(_.sampleRowKeys(testTable))

    assertIO(program, Chunk.empty)
  }

  test(
    "FunctionalBigtableDataClient.sampleRowKeys should return rows which exists"
  ) {
    sutResource.use(sut =>
      for {
        _ <- IO.delay(
          bigtableDataClient.mutateRow(
            RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
          )
        )
        res <- sut.sampleRowKeys(testTable)
      } yield {
        assertEquals(clue(res).size, 1, "Incorrect number of sampled row keys")
        assertEquals(res.head.get.getKey.toStringUtf8, key)
      }
    )
  }

  test("FunctionalBigtableDataClient.mutateRow should edit a row") {
    val keyB = "keyB"

    sutResource.use(sut =>
      for {
        beforeA <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
        beforeB <- IO.delay(Option(bigtableDataClient.readRow(testTable, keyB)))
        _ <- sut.bulkMutateRows(
          BulkMutation
            .create(testTable)
            .add(key, Mutation.create().setCell(familyA, qualifierA, 1))
        )
        _ <- sut.bulkMutateRows(
          BulkMutation
            .create(testTable)
            .add(keyB, Mutation.create().setCell(familyB, qualifierA, 1))
        )
        afterA <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
        afterB <- IO.delay(Option(bigtableDataClient.readRow(testTable, keyB)))
      } yield {
        assertEquals(beforeA, Option.empty, "Row A existed already")
        assertEquals(beforeB, Option.empty, "Row B existed already")

        assertNotEquals(afterA, Option.empty[Row], "Row A didn't exist after")
        assertEquals(afterA.get.getKey.toStringUtf8, key)

        assertNotEquals(afterB, Option.empty[Row], "Row B didn't exist after")
        assertEquals(afterB.get.getKey.toStringUtf8, keyB)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.bulkMutateRows should edit multiple rows"
  ) {
    sutResource.use(sut =>
      for {
        before <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
        _ <- sut.mutateRow(
          RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
        )
        after <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
      } yield {
        assertEquals(before, Option.empty, "Row existed already")
        assertNotEquals(after, Option.empty[Row], "Row didn't exist after")
        assertEquals(after.get.getKey.toStringUtf8, key)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.bulkMutateRowsBatcher should edit multiple rows"
  ) {
    val keyB = "keyB"

    sutResource
      .flatMap(_.bulkMutateRowsBatcher(testTable))
      .use(batcher =>
        for {
          beforeA <- IO
            .delay(Option(bigtableDataClient.readRow(testTable, key)))
          beforeB <- IO
            .delay(Option(bigtableDataClient.readRow(testTable, keyB)))
          awaitA <- batcher
            .run(RowMutationEntry.create(key).setCell(familyA, qualifierA, 1))
          awaitB <- batcher
            .run(RowMutationEntry.create(keyB).setCell(familyB, qualifierA, 1))
          midA <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
          midB <- IO.delay(Option(bigtableDataClient.readRow(testTable, keyB)))
          _ <- awaitA
          _ <- awaitB
          afterA <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
          afterB <- IO
            .delay(Option(bigtableDataClient.readRow(testTable, keyB)))
        } yield {
          assertEquals(beforeA, Option.empty, "Row A existed already")
          assertEquals(beforeB, Option.empty, "Row B existed already")

          assertEquals(midA, Option.empty, "Row A existed before await")
          assertEquals(midB, Option.empty, "Row B existed before await")

          assertNotEquals(afterA, Option.empty[Row], "Row A didn't exist after")
          assertEquals(afterA.get.getKey.toStringUtf8, key)

          assertNotEquals(afterB, Option.empty[Row], "Row B didn't exist after")
          assertEquals(afterB.get.getKey.toStringUtf8, keyB)
        }
      )
  }

  test(
    "FunctionalBigtableDataClient.bulkReadRowsBatcher should read rows, returning None if the row does not exist"
  ) {
    sutResource
      .flatMap(_.bulkReadRowsBatcher(testTable, None))
      .use(batcher =>
        for {
          before <- batcher.run(keyByteString).flatten
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- batcher.run(keyByteString).flatten
        } yield {
          assertEquals(before, None, "Row existed already")

          assert(clue(res).isDefined, "Row was not found")
          assertEquals(res.get.getKey.toStringUtf8, key)
        }
      )
  }

  test(
    "FunctionalBigtableDataClient.bulkReadRowsBatcher should work with a filter"
  ) {
    sutResource
      .flatMap(
        _.bulkReadRowsBatcher(
          testTable,
          Some(FILTERS.key().exactMatch("DO-NOT-MATCH"))
        )
      )
      .use(batcher =>
        for {
          before <- batcher.run(keyByteString).flatten
          _ <- IO.delay(
            bigtableDataClient.mutateRow(
              RowMutation.create(testTable, key).setCell(familyA, qualifierA, 1)
            )
          )
          res <- batcher.run(keyByteString).flatten
        } yield {
          assertEquals(before, None, "Row existed already")
          assertEquals(res, None, "Filter did not work")
        }
      )
  }

  test(
    "FunctionalBigtableDataClient.checkAndMutateRow should function as expected"
  ) {
    sutResource.use(sut =>
      for {
        _ <- IO.delay(
          bigtableDataClient.mutateRow(
            RowMutation
              .create(testTable, key)
              .setCell(familyA, qualifierA, "initial-value")
          )
        )
        // Update the value of the cell (the condition should match). If condition doesn't match delete
        _ <- sut.checkAndMutateRow(
          ConditionalRowMutation
            .create(testTable, key)
            .condition(
              FILTERS.value().exactMatch("updated-value")
            ) // value is currently `initial-value`
            .`then`(Mutation.create().deleteRow())
            .otherwise(
              Mutation.create().setCell(familyA, qualifierA, "updated-value")
            )
        )
        // Now delete the row using another conditional mutation
        mid <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
        _ <- sut.checkAndMutateRow(
          ConditionalRowMutation
            .create(testTable, key)
            .condition(FILTERS.value().exactMatch("updated-value"))
            .`then`(Mutation.create().deleteRow())
        )
        after <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
      } yield {
        assert(clue(mid).isDefined, "Row was not found")
        val row = mid.get
        assertEquals(row.getKey.toStringUtf8, key)

        val cells = row.getCells.asScala.toList
        assertEquals(
          clue(cells).size,
          2
        ) // 2 cells as we updated the value, first one is the newest

        val firstCell :: secondCell :: Nil = cells
        assertEquals(firstCell.getValue.toStringUtf8, "updated-value")
        assertEquals(secondCell.getValue.toStringUtf8, "initial-value")

        assertEquals(after, None)
      }
    )
  }

  test(
    "FunctionalBigtableDataClient.readModifyWriteRow should function as expected"
  ) {
    sutResource.use { sut =>
      val updateOperation: IO[Row] =
        sut.readModifyWriteRow(
          ReadModifyWriteRow
            .create(testTable, key)
            .append(familyA, qualifierA, "foo")
            .increment(familyB, qualifierA, 1L)
        )

      for {
        before <- IO.delay(Option(bigtableDataClient.readRow(testTable, key)))
        firstModification <- updateOperation
        secondModification <- updateOperation
        // Read only the latest cell from each column
        after <- IO.delay(
          Option(
            bigtableDataClient.readRow(
              testTable,
              key,
              FILTERS.limit().cellsPerColumn(1)
            )
          )
        )
      } yield {
        assertEquals(before, None, "Row existed already")

        val firstCells = firstModification.getCells.asScala.toList
        assertEquals(
          clue(firstCells).size,
          2,
          "Wrong number of cells after first modification"
        )

        val firstCellA :: firstCellB :: Nil = firstCells

        assertEquals(
          (
            firstCellA.getFamily,
            firstCellA.getQualifier.toStringUtf8,
            firstCellA.getValue.toStringUtf8
          ),
          (familyA, qualifierA, "foo"),
          "First cell from first modification does not have expected data"
        )

        assertEquals(
          (
            firstCellB.getFamily,
            firstCellB.getQualifier.toStringUtf8,
            Longs.fromByteArray(firstCellB.getValue.toByteArray)
          ),
          (familyB, qualifierA, 1L),
          "Second cell from first modification does not have expected data"
        )

        val secondCells = secondModification.getCells.asScala.toList
        assertEquals(
          clue(secondCells).size,
          2,
          "Wrong number of cells after second modification"
        )

        val secondCellA :: secondCellB :: Nil = secondCells

        assertEquals(
          (
            secondCellA.getFamily,
            secondCellA.getQualifier.toStringUtf8,
            secondCellA.getValue.toStringUtf8
          ),
          (familyA, qualifierA, "foofoo"),
          "Second cell from second modification does not have expected data"
        )

        assertEquals(
          (
            secondCellB.getFamily,
            secondCellB.getQualifier.toStringUtf8,
            Longs.fromByteArray(secondCellB.getValue.toByteArray)
          ),
          (familyB, qualifierA, 2L),
          "Second cell from second modification does not have expected data"
        )

        assert(clue(after).isDefined, "Row was not found from read")
        assertEquals(secondModification, after.get)
      }
    }
  }
}
