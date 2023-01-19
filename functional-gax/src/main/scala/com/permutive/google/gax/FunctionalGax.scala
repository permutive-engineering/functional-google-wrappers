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
import cats.effect.kernel._
import cats.effect.std.{Dispatcher, Semaphore}
import cats.syntax.all._
import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures, SettableApiFuture}
import com.google.api.gax.batching.Batcher
import com.google.api.gax.rpc.ServerStream
import com.google.common.util.concurrent.MoreExecutors
import fs2.Stream

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

/** Utilities to convert GAX interfaces (e.g. [[com.google.api.core.ApiFuture]]) into functional equivalents (e.g.
  * [[fs2.Stream]]).
  */
object FunctionalGax {

  /** Thread safe, functional equivalent of [[com.google.api.gax.batching.Batcher]].
    *
    * The outer effect sends the request to the [[com.google.api.gax.batching.Batcher]], the inner effect awaits the
    * response.
    */
  type FunctionalBatcher[F[_], Input, Result] = Kleisli[F, Input, F[Result]]

  /** Lift an [[com.google.api.core.ApiFuture]] into the `F[_]` context.
    *
    * @param fut
    *   the [[com.google.api.core.ApiFuture]] to lift into the `F[_]` context. Suspended in `F[_]` to avoid eager
    *   evaluation
    */
  def convertApiFuture[F[_]: Async, A](fut: F[ApiFuture[A]]): F[A] =
    // We can't use CE3 `Async[F].fromCompletableFuture` as `ApiFuture` only extends `Future`.
    // `ApiFuture.addListener` is analogous to the functionality that `CompletableFuture` includes, so we can achieve
    // the same result though.
    // For reference: https://github.com/typelevel/cats-effect/blob/d9285906a0b8f3c1b902549cae2781ebc4b90ee7/kernel/jvm/src/main/scala/cats/effect/kernel/AsyncPlatform.scala#L31
    Async[F]
      .async[A] { cb =>
        fut.flatMap { futA =>
          Sync[F].delay {
            addCallback(futA)(cb)

            // Define cancellation behaviour
            Some(
              Sync[F]
                .delay(
                  // This boolean setting is `mayInterruptIfRunning`:
                  //    `if the thread executing this task should be interrupted; otherwise, in-progress tasks are allowed
                  //    to complete`.
                  //
                  // We set this to `false` as this is mimics the implementation of `Async[F].fromCompletableFuture` in
                  // Cats Effect 3: https://github.com/typelevel/cats-effect/blob/d9285906a0b8f3c1b902549cae2781ebc4b90ee7/kernel/jvm/src/main/scala/cats/effect/kernel/AsyncPlatform.scala#L31
                  //
                  // In addition testing against Bigtable showed that it was not required for calling `cancel` on the `F[A]`
                  // effect to return immediately and slowed down the execution of this code block.
                  // See https://permutive.atlassian.net/browse/PLAT-255 for details (see links in ticket description).
                  futA.cancel(false)
                )
                .void // `.cancel` returns a bool indicating if the task was cancelled successfully so we discard this)
            )
          }
        }
      }

  /** Convert a [[com.google.api.gax.rpc.ServerStream]] to a [[fs2.Stream]] in the `F[_]` context.
    *
    * @param serverStream
    *   the [[com.google.api.gax.rpc.ServerStream]] to convert to a [[fs2.Stream]]. Suspended in `F[_]` to avoid eager
    *   evaluation
    * @param chunkSize
    *   the maximum size of chunks in the output stream
    */
  def convertServerStream[F[_]: Sync, A](
      serverStream: F[ServerStream[A]],
      chunkSize: Int
  ): Stream[F, A] =
    for {
      ss <- Stream.eval(serverStream)
      iterator <- Stream.eval(Sync[F].delay(ss.iterator().asScala))
      item <- Stream.fromBlockingIterator(iterator, chunkSize)
    } yield item

  /** Convert a [[com.google.api.gax.batching.Batcher]] into a functional equivalent, represented as a
    * [[cats.data.Kleisli Kleisli]].
    *
    * The resulting Kleisli sends the input to the underlying [[com.google.api.gax.batching.Batcher]] in two phases:
    * first queueing the input and then awaiting the response.
    *
    * The outer effect queues the input onto the internal buffer of the [[com.google.api.gax.batching.Batcher]]; after
    * this effect is evaluated the [[com.google.api.gax.batching.Batcher]] will send a batch with this input eventually.
    * The inner effect awaits the eventual result. The two effects could be flattened to await immediately, but this may
    * be harmful to performance in some situations.
    *
    * @param batcher
    *   the [[com.google.api.gax.batching.Batcher]] to convert, Suspended in `F[_]` to avoid eager evaluation
    *
    * @note
    *   Closing the resource may semantically block for some time as it flushes the current batch and awaits results
    */
  def convertBatcher[F[_]: Async, Input, Result](
      batcher: F[Batcher[Input, Result]]
  ): Resource[F, FunctionalBatcher[F, Input, Result]] =
    // Closing awaits completion of all current futures, so could block
    Resource
      .fromAutoCloseable(batcher)
      .flatMap(underlying =>
        fromSuspendedBatcher(
          SuspendedBatcher.fromBatcher(underlying),
          permits = 1
        )
      )

  /** Unsafely evaluate an effect and produce the result in an [[com.google.api.core.ApiFuture]].
    *
    * This is public as it may be useful to create an [[com.google.api.core.ApiFuture]] for testing purposes.
    */
  def unsafeToApiFuture[F[_], A](
      fa: F[A],
      dispatcher: Dispatcher[F]
  ): ApiFuture[A] = {
    // Implementation analogous to `DispatcherPlatform.unsafeToCompletableFuture`
    // Uses `SettableApiFuture` instead of a `CompletableFuture` though
    val sf: SettableApiFuture[A] = SettableApiFuture.create[A]()

    // Copied (including the next line comment) from `Dispatcher.unsafeRunAsync` which is package-private.
    // this is safe because the only invocation will be this callback
    implicit val parasitic: ExecutionContext = new ExecutionContext {
      override def execute(runnable: Runnable): Unit = runnable.run()
      override def reportFailure(t: Throwable): Unit = t.printStackTrace()
    }

    dispatcher
      .unsafeToFuture(fa)
      .onComplete(_.fold(sf.setException, sf.set))

    sf
  }

  @inline
  private def addCallback[A](futA: ApiFuture[A])(
      cb: Either[Throwable, A] => Unit
  ): Unit =
    ApiFutures.addCallback(
      futA,
      new ApiFutureCallback[A] {
        override def onFailure(t: Throwable): Unit =
          cb(Left(t))

        override def onSuccess(result: A): Unit =
          cb(Right(result))
      },
      // We use the `directExecutor` as this is the location to run the callback _after_ completion*. In our case that
      // means where to run `onFailure` and `onSuccess` which are both lightweight. The underlying library uses this
      // executor for similarly simple functions.
      //
      // This also mimics `IO.fromFuture` Cats Effect 2 which uses an immediate threadpool to handle the completion:
      // https://github.com/typelevel/cats-effect/blob/beb08dad45b1eac8006d18dfc877713619cdd2cb/core/shared/src/main/scala/cats/effect/internals/IOFromFuture.scala#L43
      //
      // * From a docstring used by `addCallback` (`Futures.addCallback`):
      //    `The executor to run {@code callback} when the future completes.`
      //
      // See docstring of `ListenableFuture.addListener` for more details on this being safe.
      // As above see https://permutive.atlassian.net/browse/PLAT-255 for further details on deciding this.
      MoreExecutors.directExecutor()
    )

  // INTERNAL: Used for testing purposes
  private[gax] def fromSuspendedBatcher[F[_]: Async, Input, Result](
      batcher: Kleisli[F, Input, ApiFuture[Result]],
      permits: Int
  ): Resource[F, FunctionalBatcher[F, Input, Result]] =
    Resource
      .eval(Semaphore(permits.toLong))
      .map(semaphore =>
        Kleisli(input =>
          MonadCancel[F].uncancelable { poll =>
            // Gate `underlying.add` behind a permit to ensure single-threaded access; the Java batcher isn't thread-safe.
            // The Monad[F].pure here is theoretically dubious as the whole point of passing F[ApiFuture[A]] to `convertApiFuture`
            // is to maintain referential transparency by delaying the execution of the async process. However, there's
            // nothing we can do about that here so ¯\_(ツ)_/¯
            // The uncancelable ensures that if the batcher succeeds then we will convert the `ApiFuture` and hence
            // propagate cancelation correctly.
            semaphore.permit
              .surround(poll(batcher.run(input)))
              .map(f => convertApiFuture(Applicative[F].pure(f)))
          }
        )
      )

  // INTERNAL: Used for testing purposes to suspend the impure call to `batcher.add` but keep the unsafe `ApiFuture`
  private[gax] object SuspendedBatcher {
    def fromBatcher[F[_]: Sync, Input, Result](
        batcher: Batcher[Input, Result]
    ): Kleisli[F, Input, ApiFuture[Result]] =
      Kleisli(input => Sync[F].blocking(batcher.add(input)))
  }

}
