package com.permutive.google.bigtable.data.trace4cats

import cats.FlatMap
import cats.data.Kleisli
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.implicits._
import com.google.api.gax.grpc.GrpcCallContext
import com.google.cloud.bigtable.data.v2.models._
import com.permutive.google.bigtable.data.FunctionalBigtableDataClient.RowKeyByteString
import com.permutive.google.bigtable.data.{BigtableDataClientSettings, FunctionalBigtableDataClient}
import com.permutive.google.gax.FunctionalGax.FunctionalBatcher
import fs2.Chunk
import trace4cats.model.AttributeValue
import trace4cats.model.AttributeValue.StringValue
import trace4cats.{SpanKind, Trace}

class TracedFunctionalBigtableDataClient[F[_]: Trace: FlatMap] private (
    settings: BigtableDataClientSettings[F],
    underlying: FunctionalBigtableDataClient[F]
) extends FunctionalBigtableDataClient[F] {
  private val trace = Trace[F]
  private val spanPrefix: String = "Bigtable"
  private val labelRowKey = "bigtable.rowKey"
  private val labelInstance = "bigtable.instance"
  private val labelTable = "bigtable.table"
  private val labelOperation = "bigtable.operation"

  implicit private def rowKeyByteStringToTraceValue(value: => RowKeyByteString): AttributeValue = StringValue(
    value.toStringUtf8
  )

  override def exists(tableId: String, rowKey: String): F[Boolean] =
    trace.span(s"$spanPrefix exists($tableId, ...)", SpanKind.Client)(
      trace.putAll(
        labelRowKey -> rowKey,
        labelInstance -> settings.instanceId,
        labelTable -> tableId,
        labelOperation -> "exists"
      ) >> underlying.exists(tableId, rowKey)
    )

  override def exists(tableId: String, rowKey: RowKeyByteString): F[Boolean] =
    trace.span(s"$spanPrefix exists($tableId, ...)", SpanKind.Client)(
      trace.putAll(
        labelRowKey -> rowKey,
        labelInstance -> settings.instanceId,
        labelTable -> tableId,
        labelOperation -> "exists"
      ) >> underlying.exists(tableId: String, rowKey)
    )

  override def readRow(tableId: String, rowKey: String, filter: Option[Filters.Filter]): F[Option[Row]] =
    trace.span(s"$spanPrefix readRow($tableId, ...)", SpanKind.Client)(
      trace.putAll(
        labelRowKey -> rowKey,
        labelInstance -> settings.instanceId,
        labelTable -> tableId,
        labelOperation -> "readRow"
      ) >> underlying.readRow(tableId, rowKey, filter)
    )

  override def readRow(tableId: String, rowKey: RowKeyByteString, filter: Option[Filters.Filter]): F[Option[Row]] =
    trace.span(s"$spanPrefix readRow($tableId, ...)", SpanKind.Client)(
      trace.putAll(
        labelRowKey -> rowKey,
        labelInstance -> settings.instanceId,
        labelTable -> tableId,
        labelOperation -> "readRow"
      ) >> underlying.readRow(tableId, rowKey, filter)
    )

  /** @inheritdoc
    *
    * Note that, because of the difficultly in tracing streams, a span is only open whilst the stream is being
    * _created_. This is likely to be instantaneous, and will not track the time spent consuming each item of the
    * stream.
    */
  override def readRows(query: Query, streamChunkSize: Int): fs2.Stream[F, Row] =
    fs2.Stream
      .eval(
        trace.span(s"$spanPrefix readRows(...)", SpanKind.Client)(
          trace
            .putAll(
              labelInstance -> settings.instanceId,
              "bigtable.query" -> query.toString,
              labelOperation -> "readRows"
            )
            .map(_ => underlying.readRows(query, streamChunkSize))
        )
      )
      .flatten

  override def sampleRowKeys(tableId: String): F[Chunk[KeyOffset]] =
    trace.span(s"$spanPrefix sampleRowKeys($tableId)", SpanKind.Client)(
      trace.putAll(
        labelInstance -> settings.instanceId,
        labelTable -> tableId,
        labelOperation -> "sampleRowKeys"
      ) >> underlying.sampleRowKeys(tableId)
    )

  override def mutateRow(rowMutation: RowMutation): F[Unit] =
    trace.span(s"$spanPrefix mutateRow(...)", SpanKind.Client)(
      trace.putAll(
        labelInstance -> settings.instanceId,
        labelOperation -> "mutateRow"
      ) >> underlying.mutateRow(rowMutation)
    )

  override def bulkMutateRows(bulkMutation: BulkMutation): F[Unit] =
    trace.span(s"$spanPrefix bulkMutateRows(...)", SpanKind.Client)(
      underlying.bulkMutateRows(bulkMutation)
    )

  override def bulkMutateRowsBatcher(
      tableId: String,
      grpcCallContext: Option[GrpcCallContext]
  ): Resource[F, FunctionalBatcher[F, RowMutationEntry, Unit]] =
    underlying
      .bulkMutateRowsBatcher(tableId, grpcCallContext)
      .map(underlyingBatcher =>
        Kleisli(mutationEntry =>
          underlyingBatcher
            .apply(mutationEntry)
            .map(completionF =>
              trace.span(s"$spanPrefix bulkMutateRows($tableId)", SpanKind.Client)(
                trace.putAll(
                  labelInstance -> settings.instanceId,
                  labelTable -> tableId,
                  labelOperation -> "bulkMutateRows"
                ) >> completionF
              )
            )
        )
      )

  override def bulkReadRowsBatcher(
      tableId: String,
      filter: Option[Filters.Filter],
      grpcCallContext: Option[GrpcCallContext]
  ): Resource[F, FunctionalBatcher[F, RowKeyByteString, Option[Row]]] =
    underlying
      .bulkReadRowsBatcher(tableId, filter, grpcCallContext)
      .map(underlyingBatcher =>
        Kleisli(rowKey =>
          underlyingBatcher
            .apply(rowKey)
            .map(resultF =>
              trace.span(s"$spanPrefix bulkReadRows($tableId)", SpanKind.Client)(
                trace.putAll(
                  labelRowKey -> rowKey,
                  labelInstance -> settings.instanceId,
                  labelTable -> tableId,
                  labelOperation -> "bulkReadRows"
                ) >> resultF
              )
            )
        )
      )

  override def checkAndMutateRow(mutation: ConditionalRowMutation): F[Boolean] =
    trace.span(s"$spanPrefix checkAndMutateRow(...)", SpanKind.Client)(
      trace.putAll(
        labelInstance -> settings.instanceId,
        labelOperation -> "checkAndMutateRow"
      ) >> underlying.checkAndMutateRow(mutation)
    )

  override def readModifyWriteRow(mutation: ReadModifyWriteRow): F[Row] =
    trace.span(s"$spanPrefix readModifyWriteRow(...)", SpanKind.Client)(
      trace.putAll(
        labelInstance -> settings.instanceId,
        labelOperation -> "readModifyWriteRow"
      ) >> underlying.readModifyWriteRow(mutation)
    )
}

/** Wraps a [[FunctionalBigtableDataClient]], adding trace spans and labels for each call.
  */
object TracedFunctionalBigtableDataClient {
  def resource[F[_]: Trace: Async](
      settings: BigtableDataClientSettings[F]
  ): Resource[F, TracedFunctionalBigtableDataClient[F]] =
    FunctionalBigtableDataClient
      .resource(settings)
      .map(TracedFunctionalBigtableDataClient(settings, _))

  def apply[F[_]: Trace: FlatMap](
      settings: BigtableDataClientSettings[F],
      underlying: FunctionalBigtableDataClient[F]
  ): TracedFunctionalBigtableDataClient[F] = new TracedFunctionalBigtableDataClient[F](settings, underlying)

}
