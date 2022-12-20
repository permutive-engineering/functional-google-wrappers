package com.permutive.google.bigtable.data

import cats.Applicative
import cats.syntax.all._
import cats.effect.kernel.{Resource, Sync}
import com.google.api.gax.batching.{BatchingSettings => JBatchingSettings}
import com.google.cloud.bigtable.data.v2.{
  BigtableDataSettings,
  BigtableDataClient => JBigtableDataClient
}

import scala.util.chaining._

/** Helper to construct an underlying Java Bigtable client from the provided
  * [[BigtableDataClientSettings]].
  */
object BigtableDataClientResource {

  /** Construct an underlying Java Bigtable client from the provided
    * [[BigtableDataClientSettings]].
    */
  def apply[F[_]: Sync](
      settings: BigtableDataClientSettings[F]
  ): Resource[F, JBigtableDataClient] =
    resource(settings)

  /** Construct an underlying Java Bigtable client from the provided
    * [[BigtableDataClientSettings]].
    */
  def resource[F[_]: Sync](
      settings: BigtableDataClientSettings[F]
  ): Resource[F, JBigtableDataClient] =
    Resource.fromAutoCloseable(for {
      builder <- Sync[F].delay(settings.endpoint match {
        case Some(EndpointSettings(host, port, isEmulator)) =>
          if (isEmulator) {
            BigtableDataSettings.newBuilderForEmulator(host.value, port.value)
          } else {
            val b = BigtableDataSettings.newBuilder()
            b.stubSettings().setEndpoint(s"$host:$port")
            b
          }
        case None => BigtableDataSettings.newBuilder()
      })
      finalSettings <-
        builder
          .pipe(b => settings.applicationProfile.fold(b)(b.setAppProfileId))
          .pipe(
            setReadRowsBatchingSettings(settings.readRowsBatchingSettings, _)
          )
          .pipe(
            setMutateRowsBatchingSettings(
              settings.mutateRowsBatchingSettings,
              _
            )
          )
          .pipe(
            _.setProjectId(settings.projectId.value)
              .setInstanceId(settings.instanceId)
          )
          .pipe { builder =>
            settings.endpoint match {
              // If the endpoint is an emulator do not apply the old channel provider settings
              case Some(EndpointSettings(_, _, true)) =>
                settings.modifySettings
                  .fold(Applicative[F].pure(builder))(_(builder))
              // In any other situation apply the old channel provider settings as long as the user has not provided their own
              case _ =>
                settings.modifySettings.getOrElse(
                  BigtableDataClientSettings.oldChannelProviderSettings
                )(builder)
            }
          }
          .map(_.build())
      client <- Sync[F].delay(JBigtableDataClient.create(finalSettings))
    } yield client)

  private def setBatchingSettings(
      settings: Option[BatchingSettings],
      builder: BigtableDataSettings.Builder,
      getBatchSettings: BigtableDataSettings.Builder => JBatchingSettings.Builder,
      setBatchingSettings: (
          BigtableDataSettings.Builder,
          JBatchingSettings
      ) => BigtableDataSettings.Builder
  ): BigtableDataSettings.Builder =
    settings match {
      case Some(batching) if batching.enableBatching =>
        setBatchingSettings(
          builder,
          getBatchSettings(builder)
            .pipe(b =>
              batching.requestByteThreshold.fold(b)(v =>
                b.setRequestByteThreshold(v.value)
              )
            )
            .pipe(b =>
              batching.delayThreshold.fold(b)(v =>
                b.setDelayThreshold(
                  org.threeten.bp.Duration.ofMillis(v.toMillis)
                )
              )
            )
            .pipe(b =>
              batching.elementCountThreshold.fold(b)(v =>
                b.setElementCountThreshold(v.value)
              )
            )
            .build()
        )
      case _ =>
        builder
    }

  private def setReadRowsBatchingSettings(
      settings: Option[BatchingSettings],
      builder: BigtableDataSettings.Builder
  ): BigtableDataSettings.Builder =
    setBatchingSettings(
      settings,
      builder,
      builder =>
        builder
          .stubSettings()
          .bulkReadRowsSettings()
          .getBatchingSettings
          .toBuilder,
      (builder, settings) => {
        builder
          .stubSettings()
          .bulkReadRowsSettings()
          .setBatchingSettings(settings)
        builder
      }
    )

  private def setMutateRowsBatchingSettings(
      settings: Option[BatchingSettings],
      builder: BigtableDataSettings.Builder
  ): BigtableDataSettings.Builder =
    setBatchingSettings(
      settings,
      builder,
      builder =>
        builder
          .stubSettings()
          .bulkMutateRowsSettings()
          .getBatchingSettings
          .toBuilder,
      (builder, settings) => {
        builder
          .stubSettings()
          .bulkMutateRowsSettings()
          .setBatchingSettings(settings)
        builder
      }
    )
}
