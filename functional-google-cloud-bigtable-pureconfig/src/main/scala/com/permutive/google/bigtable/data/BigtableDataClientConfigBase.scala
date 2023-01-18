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
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.permutive.google.gcp.types.ProjectId

import scala.concurrent.duration.FiniteDuration
import scala.util.chaining._

private[data] trait BigtableDataClientConfigBase {
  def projectId: ProjectId
  def instanceId: String
  def endpoint: Option[EndpointConfigBase]
  def applicationProfile: Option[String]
  def readRowsBatching: Option[BatchingConfigBase]
  def mutateRowsBatching: Option[BatchingConfigBase]

  def toCoreConfig[F[_]](
      modifySettings: Option[
        BigtableDataSettings.Builder => F[BigtableDataSettings.Builder]
      ] = None
  ): BigtableDataClientSettings[F] =
    BigtableDataClientSettings[F](projectId, instanceId)
      .pipe(settings => modifySettings.fold(settings)(settings.modifySettings))
      .pipe(settings => applicationProfile.fold(settings)(settings.withApplicationProfile))
}

private[data] trait EndpointConfigBase {
  def host: String
  def port: Port
  def isEmulator: Boolean

  def toEndpointSettings: EndpointSettings =
    if (isEmulator) EndpointSettings.emulator(host, port) else EndpointSettings(host, port)
}

private[data] trait BatchingConfigBase {
  def enableBatching: Boolean
  def requestByteThreshold: Option[Long]
  def delayThreshold: Option[FiniteDuration]
  def elementCountThreshold: Option[Long]

  def toBatchingSettings: BatchingSettings =
    (if (enableBatching) BatchingSettings.enabled
     else BatchingSettings.disabled)
      .pipe(settings => requestByteThreshold.fold(settings)(settings.withRequestByteThreshold))
      .pipe(settings => delayThreshold.fold(settings)(settings.withDelayThreshold))
      .pipe(settings => elementCountThreshold.fold(settings)(settings.withElementCountThreshold))
}
