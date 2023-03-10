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
  *   [[BatchingSettings]] for an explanation of batching settings and the default values.
  *
  * @param projectId
  *   the target GCP project
  * @param instanceId
  *   the target Bigtable instance
  * @param endpoint
  *   optional settings to configure where the target Bigtable instance is located. If not supplied then live/real
  *   Bigtable will be used (hosted by Google)
  * @param settingsModifier
  *   optional settings to configure the underlying builder. If not supplied and the target instance is not an emulator
  *   then the [[BigtableDataClientSettings#oldChannelProviderSettings]] will be applied
  * @param applicationProfile
  *   the application profile the client will identity itself with, requests will be handled according to that
  *   application profile. If it is not set, the 'default' profile will be used
  * @param readRowsBatchingSettings
  *   [[BatchingSettings]] to use for `bulkReadRowsBatcher`. If not supplied then default settings are used, these can
  *   be found in the docstring of [[BatchingSettings]] and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkReadRowsSettings-- BigtableDataSettings.getStubSettings.bulkReadRowsSettings()]]
  * @param mutateRowsBatchingSettings
  *   [[BatchingSettings]] to use for `bulkMutateRowsBatcher`. If not supplied then default settings are used, these can
  *   be found in the docstring of [[BatchingSettings]] and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkMutateRowsSettings-- BigtableDataSettings.getStubSettings.bulkMutateRowsSettings()]]
  */
sealed abstract class BigtableDataClientSettings[F[_]] private (
    val projectId: ProjectId,
    val instanceId: String,
    val endpoint: Option[EndpointSettings],
    val settingsModifier: Option[
      BigtableDataSettings.Builder => F[BigtableDataSettings.Builder]
    ],
    val applicationProfile: Option[String],
    val readRowsBatchingSettings: Option[BatchingSettings],
    val mutateRowsBatchingSettings: Option[BatchingSettings]
) {
  private def copy(
      projectId: ProjectId = projectId,
      instanceId: String = instanceId,
      endpoint: Option[EndpointSettings] = endpoint,
      settingsModifier: Option[
        BigtableDataSettings.Builder => F[BigtableDataSettings.Builder]
      ] = settingsModifier,
      applicationProfile: Option[String] = applicationProfile,
      readRowsBatchingSettings: Option[BatchingSettings] = readRowsBatchingSettings,
      mutateRowsBatchingSettings: Option[BatchingSettings] = mutateRowsBatchingSettings
  ): BigtableDataClientSettings[F] = new BigtableDataClientSettings[F](
    projectId,
    instanceId,
    endpoint,
    settingsModifier,
    applicationProfile,
    readRowsBatchingSettings,
    mutateRowsBatchingSettings
  ) {}

  /** Provide optional endpoint configuration
    * @param endpoint
    *   [[EndpointSettings]] object
    * @return
    *   updated [[BigtableDataClientSettings]]
    */
  def withEndpoint(endpoint: EndpointSettings): BigtableDataClientSettings[F] =
    copy(endpoint = Some(endpoint))

  /** Provide a function to modify the underlying Java client settings
    * @param f
    *   function to modify settings, allowing for some side effect captured by `F`
    * @return
    *   updated [[BigtableDataClientSettings]]
    */
  def modifySettings(
      f: BigtableDataSettings.Builder => F[BigtableDataSettings.Builder]
  ): BigtableDataClientSettings[F] = copy(settingsModifier = Some(f))

  /** Provide an optional application profile that the client will identity itself with, requests will be handled
    * according to that application profile
    * @param applicationProfile
    *   application profile name
    * @return
    *   updated [[BigtableDataClientSettings]]
    */
  def withApplicationProfile(
      applicationProfile: String
  ): BigtableDataClientSettings[F] =
    copy(applicationProfile = Some(applicationProfile))

  /** Provide optional settings for batching read operations.
    * @param readRowsBatchingSettings
    *   [[BatchingSettings]] object
    * @return
    *   updated [[BigtableDataClientSettings]]
    */
  def withReadRowsBatchingSettings(
      readRowsBatchingSettings: BatchingSettings
  ): BigtableDataClientSettings[F] =
    copy(readRowsBatchingSettings = Some(readRowsBatchingSettings))

  /** Provide optional settings for batching mutate operations.
    *
    * @param mutateRowsBatchingSettings
    *   [[BatchingSettings]] object
    * @return
    *   updated [[BigtableDataClientSettings]]
    */
  def mutateRowsBatchingSettings(
      mutateRowsBatchingSettings: BatchingSettings
  ): BigtableDataClientSettings[F] =
    copy(mutateRowsBatchingSettings = Some(mutateRowsBatchingSettings))
}

object BigtableDataClientSettings {

  /** Channel provider settings for the Bigtable client as existed in the underling Java library before `1.16.0`.
    *
    * These setting are applied by default to all underlying clients unless
    * [[BigtableDataClientSettings.settingsModifier]] is specified.
    *
    * Version `1.16.0` introduced changes which led to `GOAWAY` being observed in production. As a result these changes
    * we "reverted" manually by us: https://github.com/permutive/permutive-utils/pull/97
    *
    * Version release: https://github.com/googleapis/java-bigtable/releases/tag/v1.16.0 Changes:
    * https://github.com/googleapis/java-bigtable/pull/409
    */
  def oldChannelProviderSettings[F[_]: Sync]: BigtableDataSettings.Builder => F[BigtableDataSettings.Builder] =
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

  /** Create an instance of [[BigtableDataClientSettings]] with the required fields provided
    *
    * @param projectId
    *   the target GCP project
    * @param instanceId
    *   the target Bigtable instance
    * @return
    *   a new instance of [[BigtableDataClientSettings]]
    */
  def apply[F[_]](
      projectId: ProjectId,
      instanceId: String
  ): BigtableDataClientSettings[F] = new BigtableDataClientSettings[F](
    projectId,
    instanceId,
    None,
    None,
    None,
    None,
    None
  ) {}
}

/** Settings to configure the location of Bigtable, if live Bigtable is not used.
  *
  * @param host
  *   host of target Bigtable instance
  * @param port
  *   port of the target Bigtable instance
  * @param isEmulator
  *   whether the target Bigtable instance is an emulator nor not
  */
sealed abstract class EndpointSettings private (
    val host: String,
    val port: Port,
    val isEmulator: Boolean
)

object EndpointSettings {

  /** Create a new [[EndpointSettings]] for a real Bigtable instance
    * @param host
    *   hostname of the instance
    * @param port
    *   port number of the instance
    * @return
    *   new [[EndpointSettings]] with `isEmulator` set to `false`
    */
  def apply(host: String, port: Port): EndpointSettings =
    new EndpointSettings(host, port, isEmulator = false) {}

  /** Create a new [[EndpointSettings]] for a Bigtable emulator
    *
    * @param host
    *   hostname of the emulator
    * @param port
    *   port number of the emulator
    * @return
    *   new [[EndpointSettings]] with `isEmulator` set to `true`
    */
  def emulator(host: String, port: Port): EndpointSettings =
    new EndpointSettings(host, port, isEmulator = true) {}
}

/** Settings to configure batching thresholds.
  *
  * These settings are a Scala representation of [[com.google.api.gax.batching.BatchingSettings BatchingSettings]] in
  * the underlying Java library.
  *
  * When batching, if any threshold is passed then a request will be sent. Note that these are thresholds, not limits,
  * so a request can be sent that is over the threshold for bytes or element counts. As an example with bytes: if the
  * current total size of queued elements is `a` and another element is added with size `b` then a request of size `a +
  * b` will be sent if `a + b` is over the bytes threshold.
  *
  * @note
  *   default settings are visible in the underlying Java library docstrings. See
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkMutateRowsSettings-- BigtableDataSettings.getStubSettings.bulkMutateRowsSettings()]]
  *   for default mutate batching settings, and
  *   [[https://googleapis.dev/java/google-cloud-bigtable/latest/com/google/cloud/bigtable/data/v2/stub/EnhancedBigtableStubSettings.html#bulkReadRowsSettings-- BigtableDataSettings.getStubSettings.bulkReadRowsSettings()]]
  *   for default read batching settings. Defaults are also specified in the parameter here, but these may be out of
  *   date.
  *
  * @param enableBatching
  *   whether batching should be enabled or not
  * @param requestByteThreshold
  *   threshold total number of bytes in queued elements to trigger a request. If not specified the default value is
  *   400KB for read and 20MB for mutate.
  * @param delayThreshold
  *   delay after first element is queued to send a request, even if no other threshold has been reached. If not
  *   specified the default value is 1 second for both read and mutate.
  * @param elementCountThreshold
  *   threshold total number of queued elements to trigger a request. If not specified the default value is 100 elements
  *   for both read and mutate.
  */
sealed abstract class BatchingSettings private (
    val enableBatching: Boolean,
    val requestByteThreshold: Option[Long],
    val delayThreshold: Option[FiniteDuration],
    val elementCountThreshold: Option[Long]
) {
  private def copy(
      enableBatching: Boolean = enableBatching,
      requestByteThreshold: Option[Long] = requestByteThreshold,
      delayThreshold: Option[FiniteDuration] = delayThreshold,
      elementCountThreshold: Option[Long] = elementCountThreshold
  ) = new BatchingSettings(
    enableBatching,
    requestByteThreshold,
    delayThreshold,
    elementCountThreshold
  ) {}

  /** Add a request byte threshold to specify total number of bytes in queued elements to trigger a request
    * @param requestByteThreshold
    *   threshold
    * @return
    *   updated [[BatchingSettings]] instance
    */
  def withRequestByteThreshold(requestByteThreshold: Long): BatchingSettings =
    copy(requestByteThreshold = Some(requestByteThreshold))

  /** Add a delay threshold to specify delay after first element is queued to send a request.
    * @param delayThreshold
    *   the time to delay before sending the batch
    * @return
    *   updated [[BatchingSettings]] instance
    */
  def withDelayThreshold(delayThreshold: FiniteDuration): BatchingSettings =
    copy(delayThreshold = Some(delayThreshold))

  /** Add a request byte threshold to specify total number of elements to trigger a request
    *
    * @param elementCountThreshold
    *   threshold
    * @return
    *   updated [[BatchingSettings]] instance
    */
  def withElementCountThreshold(elementCountThreshold: Long): BatchingSettings =
    copy(elementCountThreshold = Some(elementCountThreshold))
}

object BatchingSettings {

  /** Create a [[BatchingSettings]] instance with batching enabled
    * @return
    *   a new [[BatchingSettings]] instance
    */
  def enabled: BatchingSettings =
    new BatchingSettings(enableBatching = true, None, None, None) {}

  /** Create a [[BatchingSettings]] instance with batching disabled
    *
    * @return
    *   a new [[BatchingSettings]] instance
    */
  def disabled: BatchingSettings =
    new BatchingSettings(enableBatching = false, None, None, None) {}
}
