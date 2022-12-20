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
import cats.effect.IO
import com.google.cloud.bigtable.data.v2.models.{Row, RowMutation}
import com.google.common.primitives.Longs

import scala.jdk.CollectionConverters._

class BigtableSuiteSpec extends BigtableSuite {

  val testTable = "test-table"
  val columnFamily = "column-family"
  val columnQualifier = "column-qualifier"
  val key = "key"
  val data = 1L

  val readRow: IO[Option[Row]] =
    IO.delay(Option(bigtableDataClient.readRow(testTable, key)))

  override lazy val tablesAndColumnFamilies
      : NonEmptyMap[String, NonEmptySet[String]] =
    NonEmptyMap.one("test-table", NonEmptySet.one(columnFamily))

  test("BigtableSuite should support data retrieval") {
    for {
      _ <- IO.delay(
        bigtableDataClient.mutateRow(
          RowMutation
            .create(testTable, key)
            .setCell(columnFamily, columnQualifier, data)
        )
      )
      rowO <- readRow
    } yield {
      assert(clue(rowO).isDefined, "Row was not found")
      val row = rowO.get

      assertEquals(row.getKey.toStringUtf8, key)

      val cells = row.getCells.asScala.toList
      assertEquals(clue(cells).length, 1, "More than 1 cell was found")

      val cell = cells.head

      assertEquals(cell.getFamily, columnFamily)
      assertEquals(cell.getQualifier.toStringUtf8, columnQualifier)
      assertEquals(Longs.fromByteArray(cell.getValue.toByteArray), data)
    }
  }

  test("BigtableSuite should delete data between test runs") {
    assertIO(readRow, None)
  }
}
