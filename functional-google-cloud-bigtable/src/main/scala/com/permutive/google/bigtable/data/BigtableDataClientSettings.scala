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

import cats.effect.kernel.Sync
import cats.syntax.all._
import com.comcast.ip4s.Port
import com.google.api.gax.grpc.ChannelPoolSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.stub.BigtableStubSettings
import com.permutive.google.gcp.types.ProjectId

import scala.concurrent.duration.FiniteDuration

/** Settings to construct an underlying Java Bigtable client.
  *
  * @see
  *   [[BigtableDataClientResource]] to construct the underlying client.
  * @see
  *   [[BatchingSettings]] for an explanation of batching settings and the
  *   default values.
  *
  * @param projectId
  *   the target GCP project
  * @param instanceId
  *   the target Bigtable instance
  * @param endpoint
  *   optional settings to configure where the target Bigtable instance is
  *   located. If not supplied then live/real Bigtable will be used (hosted by
  *   Google)
  * @param modifySettings
  *   optional settings to configure the underlying builder. If not supplied and
  *   the target instance is not an emulator then the
  *   [[BigtableDataClientSettings#oldChannelProviderSettings]] will be applied
  * @param applicationProfile
  *   the application profile the client will identity itself with, requests
  *   will be handled according to that application profile. If it is not set,
  *   the 'default' profile will be used
  * @param readRowsBatchingSettings
  *   [[BatchingSettings]] to use for `bulkReadRowsBatcher`. If not supplied
  *   then default settings are used, these can be found in the docstring of
  *   [[BatchingSettings]] and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkReadRowsSettings-- BigtableDataSettings.getStubSettings.bulkReadRowsSettings()]]
  * @param mutateRowsBatchingSettings
  *   [[BatchingSettings]] to use for `bulkMutateRowsBatcher`. If not supplied
  *   then default settings are used, these can be found in the docstring of
  *   [[BatchingSettings]] and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkMutateRowsSettings-- BigtableDataSettings.getStubSettings.bulkMutateRowsSettings()]]
  */
case class BigtableDataClientSettings[F[_]](
    projectId: ProjectId,
    instanceId: String,
    endpoint: Option[EndpointSettings] = None,
    modifySettings: Option[
      BigtableDataSettings.Builder => F[BigtableDataSettings.Builder]
    ] = None,
    applicationProfile: Option[String] = None,
    readRowsBatchingSettings: Option[BatchingSettings] = None,
    mutateRowsBatchingSettings: Option[BatchingSettings] = None
)

object BigtableDataClientSettings {

  /** Channel provider settings for the Bigtable client as existed in the
    * underling Java library before `1.16.0`.
    *
    * These setting are applied by default to all underlying clients unless
    * [[BigtableDataClientSettings.modifySettings]] is specified.
    *
    * Version `1.16.0` introduced changes which led to `GOAWAY` being observed
    * in production. As a result these changes we "reverted" manually by us:
    * https://github.com/permutive/permutive-utils/pull/97
    *
    * Version release:
    * https://github.com/googleapis/java-bigtable/releases/tag/v1.16.0 Changes:
    * https://github.com/googleapis/java-bigtable/pull/409
    */
  def oldChannelProviderSettings[F[_]: Sync]
      : BigtableDataSettings.Builder => F[BigtableDataSettings.Builder] =
    settings =>
      for {
        channelProvider <-
          Sync[F]
            .delay(
              // restore the channel provider settings to pre 0.16.0 version
              BigtableStubSettings
                .defaultGrpcTransportProviderBuilder()
                .setChannelPoolSettings(
                  ChannelPoolSettings.staticallySized(
                    2 * Runtime.getRuntime.availableProcessors()
                  )
                )
                .setMaxInboundMessageSize(256 * 1024 * 1024)
                .setAttemptDirectPath(
                  java.lang.Boolean.getBoolean("bigtable.attempt-directpath")
                )
                .build()
            )
        updated <- Sync[F].delay {
          // the stub settings are mutable so making the change like this _should_ work
          settings.stubSettings().setTransportChannelProvider(channelProvider)
          settings
        }
      } yield updated

}

/** Settings to configure the location of Bigtable, if live Bigtable is not
  * used.
  *
  * @param host
  *   host of target Bigtable instance
  * @param port
  *   port of the target Bigtable instance
  * @param isEmulator
  *   whether the target Bigtable instance is an emulator nor not
  */
case class EndpointSettings(
    host: String,
    port: Port,
    isEmulator: Boolean = true
)

/** Settings to configure batching thresholds.
  *
  * These settings are a Scala representation of
  * [[com.google.api.gax.batching.BatchingSettings BatchingSettings]] in the
  * underlying Java library.
  *
  * When batching, if any threshold is passed then a request will be sent. Note
  * that these are thresholds, not limits, so a request can be sent that is over
  * the threshold for bytes or element counts. As an example with bytes: if the
  * current total size of queued elements is `a` and another element is added
  * with size `b` then a request of size `a + b` will be sent if `a + b` is over
  * the bytes threshold.
  *
  * @note
  *   default settings are visible in the underlying Java library docstrings.
  *   See
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkMutateRowsSettings-- BigtableDataSettings.getStubSettings.bulkMutateRowsSettings()]]
  *   for default mutate batching settings, and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkReadRowsSettings-- BigtableDataSettings.getStubSettings.bulkReadRowsSettings()]]
  *   for default read batching settings. Defaults are also specified in the
  *   parameter here, but these may be out of date.
  *
  * @param enableBatching
  *   whether batching should be enabled or not
  * @param requestByteThreshold
  *   threshold total number of bytes in queued elements to trigger a request.
  *   If not specified the default value is 400KB for read and 20MB for mutate.
  * @param delayThreshold
  *   delay after first element is queued to send a request, even if no other
  *   threshold has been reached. If not specified the default value is 1 second
  *   for both read and mutate.
  * @param elementCountThreshold
  *   threshold total number of queued elements to trigger a request. If not
  *   specified the default value is 100 elements for both read and mutate.
  */
case class BatchingSettings(
    enableBatching: Boolean,
    requestByteThreshold: Option[Long],
    delayThreshold: Option[FiniteDuration],
    elementCountThreshold: Option[Long]
)
