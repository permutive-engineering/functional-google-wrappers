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

import com.comcast.ip4s.Port
import com.permutive.google.gcp.types.ProjectId
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Pureconfig configuration to construct an underlying Java Bigtable client.
  *
  * @param projectId
  *   the target GCP project
  * @param instanceId
  *   the target Bigtable instance
  * @param endpoint
  *   optional settings to configure where the target Bigtable instance is located. If not supplied then live/real
  *   Bigtable will be used (hosted by Google)
  * @param readRowsBatching
  *   batching configuration for `bulkReadRowsBatcher`
  * @param mutateRowsBatching
  *   batching configuration for `bulkMutateRowsBatcher`
  */
case class BigtableDataClientConfig(
    projectId: ProjectId,
    instanceId: String,
    endpoint: Option[EndpointConfig] = None,
    applicationProfile: Option[String] = None,
    readRowsBatching: Option[BatchingConfig] = None,
    mutateRowsBatching: Option[BatchingConfig] = None
) extends BigtableDataClientConfigBase

object BigtableDataClientConfig {
  implicit val projectIdReader: ConfigReader[ProjectId] =
    ConfigReader.fromStringOpt(ProjectId.fromString)

  implicit val reader: ConfigReader[BigtableDataClientConfig] =
    deriveReader[BigtableDataClientConfig]
}

/** Configuration of the location of Bigtable, if live Bigtable is not used.
  *
  * @param host
  *   host of target Bigtable instance
  * @param port
  *   port of the target Bigtable instance
  * @param isEmulator
  *   whether the target Bigtable instance is an emulator nor not
  */
case class EndpointConfig(host: String, port: Port, isEmulator: Boolean = true) extends EndpointConfigBase

object EndpointConfig {
  implicit val portReader: ConfigReader[Port] = ConfigReader.fromStringOpt(Port.fromString)

  implicit val reader: ConfigReader[EndpointConfig] =
    deriveReader[EndpointConfig]
}

/** Configuration for batching thresholds.
  *
  * This configuration is a Scala representation of [[com.google.api.gax.batching.BatchingSettings BatchingSettings]] in
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
case class BatchingConfig(
    // This probably needs to exist for clarity. An empty object would be valid otherwise (indicating batching should
    // occur)and this could be confused with a `null` (which means "do not batch").
    enableBatching: Boolean,
    requestByteThreshold: Option[Long],
    delayThreshold: Option[FiniteDuration],
    elementCountThreshold: Option[Long]
) extends BatchingConfigBase

object BatchingConfig {
  implicit val reader: ConfigReader[BatchingConfig] = deriveReader[BatchingConfig]
}
