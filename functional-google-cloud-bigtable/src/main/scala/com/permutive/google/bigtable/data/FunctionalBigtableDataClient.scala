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

import cats.effect.kernel.{Async, Resource, Sync}
import cats.syntax.all._
import com.google.api.core.ApiFuture
import com.google.api.gax.batching.Batcher
import com.google.api.gax.grpc.GrpcCallContext
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models.Filters.Filter
import com.google.cloud.bigtable.data.v2.models._
import com.google.cloud.bigtable.data.v2.{
  BigtableDataClient => JBigtableDataClient
}
import com.google.protobuf.ByteString
import com.permutive.google.gax.FunctionalGax
import com.permutive.google.gax.FunctionalGax.FunctionalBatcher
import fs2.{Chunk, Stream}

import scala.jdk.CollectionConverters._

/** Thin wrapper around
  * [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient]]
  * with functional effects and safer response types.
  *
  * Handles `null` values in the underlying client response types and converts
  * collections to Scala equivalents. Does _not_ modify the responses any
  * further though. For example,
  * [[com.google.cloud.bigtable.data.v2.models.Row Row]] will responses will
  * still contain Java collections.
  *
  * Exposes all underlying functionality of the Java client, but not all
  * methods. It reduces the number of methods by:
  *   - Not exposing blocking versions of methods, only non-blocking (and
  *     referentially transparent) methods exist
  *   - Removing overloaded methods by using [[scala.Option Option]]
  *
  * @note
  *   Unfortunately ScalaDoc cannot link to the exact methods in
  *   `BigtableDataClient` (as that is a Java class). Instead links just go to
  *   documentation for the class, with the correct method name as the text.
  */
trait FunctionalBigtableDataClient[F[_]] {

  /** Type alias to make intentions clearer in `FunctionalBatcher` */
  type RowKeyByteString = ByteString

  /** Checks if a given row exists or not.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#exists]]
    */
  def exists(tableId: String, rowKey: String): F[Boolean]

  /** Checks if a given row exists or not.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#exists]]
    */
  def exists(tableId: String, rowKey: ByteString): F[Boolean]

  /** Read a single row with an optional filter.
    *
    * Most users will want to impose a "cells per column" limit of 1:
    * \```FILTERS.limit().cellsPerColumn(1)```
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#readRow]]
    */
  def readRow(
      tableId: String,
      rowKey: String,
      filter: Option[Filter]
  ): F[Option[Row]]

  /** Read a single row with an optional filter.
    *
    * Most users will want to impose a "cells per column" limit of 1:
    * \```FILTERS.limit().cellsPerColumn(1)```
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#readRow]]
    */
  def readRow(
      tableId: String,
      rowKey: ByteString,
      filter: Option[Filter]
  ): F[Option[Row]]

  /** Stream a series of result rows from the provided query.
    *
    * Most users will want to impose a "cells per column" limit of 1:
    * \```FILTERS.limit().cellsPerColumn(1)```
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#readRows]]
    */
  def readRows(query: Query, streamChunkSize: Int): Stream[F, Row]

  /** Sample the row keys present in the table. Returned keys delimit contiguous
    * sections, approximately equal in size.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#sampleRowKeys]]
    *
    * @note
    *   this returns a [[fs2.Chunk Chunk]] because it's the easiest immutable
    *   collection to return which wraps the underlying Java list; other
    *   collections iterate to construct.
    */
  def sampleRowKeys(tableId: String): F[Chunk[KeyOffset]]

  /** Mutate a single row atomically.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#mutateRow]]
    */
  def mutateRow(rowMutation: RowMutation): F[Unit]

  /** Mutate multiple rows in a batch. Each individual row is mutated
    * atomically.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#bulkMutateRows]]
    */
  def bulkMutateRows(bulkMutation: BulkMutation): F[Unit]

  /** Create a `FunctionalBatcher` which mutates multiple rows in a batch. Each
    * individual row is mutated atomically.
    *
    * This is thread safe, so can be accessed concurrently.
    *
    * The resulting Kleisli sends the input to the underlying
    * [[com.google.api.gax.batching.Batcher]] in two phases, each represented by
    * nested effects (`F[F[Unit]]`). The outer effect queues the input to the
    * underlying `Batcher` to be sent at some point, the inner effect then
    * semantically blocks waiting for a response.
    *
    * ==Configuring batching==
    * Configuration of this batching is controlled by
    * [[BigtableDataClientSettings.mutateRowsBatchingSettings]]. Note that this
    * is at the point this interface is constructed, not whenever this method is
    * called.
    *
    * Default settings are visible in the docstring of
    * [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkMutateRowsSettings-- BigtableDataSettings.getStubSettings.bulkMutateRowsSettings()]]
    * (note: not the builder). These are copied below but they might not stay
    * accurate:
    * {{{
    * On breach of certain triggers, the operation initiates processing of accumulated request for which the default settings are:
    *  - When the request count reaches 100.
    *  - When accumulated request size reaches to 20MB.
    *  - When an interval of 1 second passes after batching initialization or last processed batch.
    * }}}
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#newBulkMutationBatcher]]
    *
    * @note
    *   Closing the resource may semantically block for some time as it flushes
    *   the current batch and awaits results
    *
    * @note
    *   `BigtableDataSettings.Builder.enableBatchMutationLatencyBasedThrottling`
    *   may be useful as well. It dynamically alters the number of in-flight
    *   requests to achieve a target RPC latency. To use this setting you will
    *   manually need to set it in
    *   [[BigtableDataClientSettings.modifySettings]]. Note that this (I think!)
    *   does not affect batching thresholds at all; instead it only affects the
    *   number of in-flight requests allowed to target a certain latency _after_
    *   a batching threshold has been reached. I may be wrong though, these
    *   settings are very hard to understand...
    */
  def bulkMutateRowsBatcher(
      tableId: String,
      grpcCallContext: Option[GrpcCallContext] = None
  ): Resource[F, FunctionalBatcher[F, RowMutationEntry, Unit]]

  /** Create a `FunctionalBatcher` which reads multiple rows, with an optional
    * filter, in a batch.
    *
    * This is thread safe, so can be accessed concurrently.
    *
    * The resulting Kleisli sends the input to the underlying
    * [[com.google.api.gax.batching.Batcher]] in two phases, each represented by
    * nested effects (`F[F[Option[Row]]]`). The outer effect queues the input to
    * the underlying `Batcher` to be sent at some point, the inner effect then
    * semantically blocks waiting for a response.
    *
    * Most users will want to impose a "cells per column" limit of 1:
    * \```FILTERS.limit().cellsPerColumn(1)```
    *
    * ==Configuring batching==
    * Configuration of this batching is controlled by
    * [[BigtableDataClientSettings.readRowsBatchingSettings]].Note that this is
    * at the point this interface is constructed, not whenever this method is
    * called.
    *
    * Default settings are visible in the docstring of
    * [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkReadRowsSettings-- BigtableDataSettings.getStubSettings.bulkReadRowsSettings()]]
    * (note: not the builder). These are copied below but they might not stay
    * accurate:
    * {{{
    * On breach of certain triggers, the operation initiates processing of accumulated request for which the default settings are:
    *  - When the request count reaches 100.
    *  - When accumulated request size reaches to 400KB.
    *  - When an interval of 1 second passes after batching initialization or last processed batch.
    * }}}
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#newBulkReadRowsBatcher]]
    *
    * @note
    *   Closing the resource may semantically block for some time as it flushes
    *   the current batch and awaits results
    */
  def bulkReadRowsBatcher(
      tableId: String,
      filter: Option[Filter],
      grpcCallContext: Option[GrpcCallContext] = None
  ): Resource[F, FunctionalBatcher[F, RowKeyByteString, Option[Row]]]

  /** Mutate a row atomically based on the output of a filter.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#checkAndMutateRow]]
    */
  def checkAndMutateRow(mutation: ConditionalRowMutation): F[Boolean]

  /** Modify a row atomically on the server based on its current value. Returns
    * the new contents of all modified cells.
    *
    * @see
    *   [[com.google.cloud.bigtable.data.v2.BigtableDataClient BigtableDataClient#readModifyWriteRow]]
    */
  def readModifyWriteRow(mutation: ReadModifyWriteRow): F[Row]

}

object FunctionalBigtableDataClient {

  /** Construct a [[FunctionalBigtableDataClient]] given the provided settings
    * for the underlying client.
    */
  def resource[F[_]: Async](
      dataClientSettings: BigtableDataClientSettings[F]
  ): Resource[F, FunctionalBigtableDataClient[F]] =
    resource(dataClientSettings, FunctionalGax.impl)

  /** Construct a [[FunctionalBigtableDataClient]] given the provided settings
    * for the underlying client and `FunctionalGax`.
    */
  def resource[F[_]: Sync](
      dataClientSettings: BigtableDataClientSettings[F],
      functionalGax: FunctionalGax[F]
  ): Resource[F, FunctionalBigtableDataClient[F]] =
    BigtableDataClientResource(dataClientSettings).map(impl(_, functionalGax))

  /** Implementation of [[FunctionalBigtableDataClient]] from an underlying Java
    * client and `FunctionalGax`.
    */
  def impl[F[_]: Sync](
      dataClient: JBigtableDataClient,
      functionalGax: FunctionalGax[F]
  ): FunctionalBigtableDataClient[F] =
    new FunctionalBigtableDataClient[F] {

      override def exists(tableId: String, rowKey: String): F[Boolean] =
        delayConvert(dataClient.existsAsync(tableId, rowKey))
          // Convert Java => Scala, doesn't "just work" unfortunately
          .map(b => b)

      override def exists(tableId: String, rowKey: ByteString): F[Boolean] =
        delayConvert(dataClient.existsAsync(tableId, rowKey))
          // Convert Java => Scala, doesn't "just work" unfortunately
          .map(b => b)

      override def readRow(
          tableId: String,
          rowKey: String,
          filter: Option[Filter]
      ): F[Option[Row]] =
        delayConvert(dataClient.readRowAsync(tableId, rowKey, filter.orNull))
          // Result can be null
          .map(Option(_))

      override def readRow(
          tableId: String,
          rowKey: ByteString,
          filter: Option[Filter]
      ): F[Option[Row]] =
        delayConvert(dataClient.readRowAsync(tableId, rowKey, filter.orNull))
          // Result can be null
          .map(Option(_))

      override def readRows(
          query: Query,
          streamChunkSize: Int
      ): Stream[F, Row] =
        delayStream(dataClient.readRows(query), streamChunkSize)

      override def sampleRowKeys(tableId: String): F[Chunk[KeyOffset]] =
        delayConvert(dataClient.sampleRowKeysAsync(tableId)).map(jList =>
          Chunk.iterable(jList.asScala)
        )

      override def mutateRow(rowMutation: RowMutation): F[Unit] =
        delayConvert(dataClient.mutateRowAsync(rowMutation)).void

      override def bulkMutateRows(bulkMutation: BulkMutation): F[Unit] =
        delayConvert(dataClient.bulkMutateRowsAsync(bulkMutation)).void

      override def bulkMutateRowsBatcher(
          tableId: String,
          grpcCallContext: Option[GrpcCallContext] = None
      ): Resource[F, FunctionalBatcher[F, RowMutationEntry, Unit]] =
        delayBatcher(
          dataClient.newBulkMutationBatcher(tableId, grpcCallContext.orNull)
        )
          // We need to define a new batcher to convert the Java `void` to a unit
          .map(_.map(_.void))

      override def bulkReadRowsBatcher(
          tableId: String,
          filter: Option[Filter],
          grpcCallContext: Option[GrpcCallContext] = None
      ): Resource[F, FunctionalBatcher[F, RowKeyByteString, Option[Row]]] =
        delayBatcher(
          dataClient.newBulkReadRowsBatcher(
            tableId,
            filter.orNull,
            grpcCallContext.orNull
          )
        )
          // We need to define a new batcher to handle the case where the row returned is null
          .map(_.map(_.map(Option(_))))

      override def checkAndMutateRow(
          mutation: ConditionalRowMutation
      ): F[Boolean] =
        delayConvert(dataClient.checkAndMutateRowAsync(mutation))
          // Convert Java Boolean => Scala, doesn't "just work" unfortunately
          .map(b => b)

      override def readModifyWriteRow(mutation: ReadModifyWriteRow): F[Row] =
        // [Ben: 2020-06-17] Underlying method does _not_ state the response can be null. It says it returns "the new
        // contents of all modified cells".
        // Chasing the code a bit it seems to fill a default stub row if no changes were made
        delayConvert(dataClient.readModifyWriteRowAsync(mutation))

      private def delayConvert[A](a: => ApiFuture[A]): F[A] =
        functionalGax.convertApiFuture(Sync[F].delay(a))

      private def delayStream[A](
          a: => ServerStream[A],
          chunkSize: Int
      ): Stream[F, A] =
        functionalGax.convertServerStream(Sync[F].delay(a), chunkSize)

      private def delayBatcher[A, B](
          a: => Batcher[A, B]
      ): Resource[F, FunctionalBatcher[F, A, B]] =
        functionalGax.convertBatcher(Sync[F].delay(a))
    }

}
