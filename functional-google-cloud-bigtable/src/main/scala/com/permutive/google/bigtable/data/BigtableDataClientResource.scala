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

import cats.Applicative
import cats.syntax.all._
import cats.effect.kernel.{Resource, Sync}
import com.google.api.gax.batching.{BatchingSettings => JBatchingSettings}
import com.google.cloud.bigtable.data.v2.{BigtableDataSettings, BigtableDataClient => JBigtableDataClient}

import scala.util.chaining._

/** Helper to construct an underlying Java Bigtable client from the provided [[BigtableDataClientSettings]].
  */
object BigtableDataClientResource {

  /** Construct an underlying Java Bigtable client from the provided [[BigtableDataClientSettings]].
    */
  def apply[F[_]: Sync](
      settings: BigtableDataClientSettings[F]
  ): Resource[F, JBigtableDataClient] =
    resource(settings)

  /** Construct an underlying Java Bigtable client from the provided [[BigtableDataClientSettings]].
    */
  def resource[F[_]: Sync](
      settings: BigtableDataClientSettings[F]
  ): Resource[F, JBigtableDataClient] =
    Resource.fromAutoCloseable(for {
      builder <- Sync[F].delay(settings.endpoint match {
        case Some(s) =>
          if (s.isEmulator) {
            BigtableDataSettings.newBuilderForEmulator(s.host, s.port.value)
          } else {
            val b = BigtableDataSettings.newBuilder()
            b.stubSettings().setEndpoint(s"${s.host}:${s.port.value}")
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
              case Some(s) if s.isEmulator =>
                settings.settingsModifier
                  .fold(Applicative[F].pure(builder))(_(builder))
              // In any other situation apply the old channel provider settings as long as the user has not provided their own
              case _ =>
                settings.settingsModifier.getOrElse(
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
              batching.requestByteThreshold.fold(b)(
                b.setRequestByteThreshold(_)
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
              batching.elementCountThreshold.fold(b)(
                b.setElementCountThreshold(_)
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
