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

package com.permutive.google.gax

import cats.Applicative
import cats.data.Kleisli
import cats.effect.Sync
import cats.effect.kernel.{Async, Ref, Resource, Temporal}
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.google.api.core.ApiFuture

import scala.concurrent.duration.{FiniteDuration, _}

/*
 * Implementation of batching Kliesli for testing purposes.
 *
 * Records current and max inflight requests to `add`.
 */
class TestingBatcher[F[_]: Async, A, B](
    state: Ref[F, TestingBatcher.State],
    sleepDuration: FiniteDuration,
    result: A => F[B],
    dispatcher: Dispatcher[F]
) {

  val getState: F[TestingBatcher.State] = state.get

  private[this] val acquireState: Resource[F, Unit] =
    Resource.make[F, Unit](state.update(_.increment))(_ => state.update(_.decrement))

  val suspendedBatcher: Kleisli[F, A, ApiFuture[B]] =
    Kleisli(input =>
      // Increment inflight, sleep for some time, then decrement. Make this semantically blocking so we can capture
      // concurrency
      acquireState
        .surround(Temporal[F].sleep(sleepDuration))
        .flatMap(_ =>
          // Need to suspend this, or it starts eagerly evaluating
          Sync[F].delay(
            FunctionalGax.unsafeToApiFuture(result(input), dispatcher)
          )
        )
    )

}

object TestingBatcher {

  def resource[F[_]: Async, A, B](
      result: A => F[B],
      sleepDuration: FiniteDuration = 1.second
  ): Resource[F, TestingBatcher[F, A, B]] =
    for {
      ref <- Resource.eval(Ref.of(State.empty))
      dispatcher <- Dispatcher.sequential[F]
    } yield new TestingBatcher(ref, sleepDuration, result, dispatcher)

  def unit[F[_]: Async](
      sleepDuration: FiniteDuration = 1.second
  ): Resource[F, TestingBatcher[F, Unit, Unit]] =
    resource[F, Unit, Unit](_ => Applicative[F].unit, sleepDuration)

  final case class State(inflight: Int, maxInflight: Int) {
    def decrement: State = State(inflight - 1, maxInflight)
    def increment: State = {
      val newInflight = inflight + 1
      val newMax = newInflight.max(maxInflight)

      State(newInflight, newMax)
    }
  }
  object State {
    val empty = State(0, 0)
  }
}
