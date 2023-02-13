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

import cats.effect.kernel.{Async, Deferred, Resource, Sync}
import cats.effect.std.{Dispatcher, Queue}
import cats.syntax.all._
import com.google.api.gax.rpc.{ResponseObserver, StreamController}
import fs2.Stream

//TODO: tests
private[gax] object ResponseStream {

  def stream[F[_]: Async, A](
      listen: ResponseObserver[A] => F[Unit],
      chunkSize: Int
  ): Stream[F, A] = for {
    dispatcher <- Stream.resource(Dispatcher.sequential[F])
    controller <- Stream.eval(Deferred[F, StreamController])
    queue <- Stream.eval(Queue.bounded[F, Option[A]](chunkSize))
    err <- Stream.eval(Deferred[F, Throwable])
    observer = new QueuedResponseObserver[F, A](
      queue,
      dispatcher,
      chunkSize,
      err,
      controller
    )
    _ <- Stream.bracketCase(listen(observer)) {
      // Cancellation added here once ResponseObserver#onStart should have been called
      case (_, Resource.ExitCase.Canceled) => observer.cancel
      case (_, _) => Sync[F].unit
    }
    res <- observer.stream
  } yield res

  private[ResponseStream] class QueuedResponseObserver[F[_]: Sync, A] private[ResponseStream] (
      queue: Queue[F, Option[A]],
      dispatcher: Dispatcher[F],
      chunkSize: Int,
      err: Deferred[F, Throwable],
      controller: Deferred[F, StreamController]
  ) extends ResponseObserver[A] {

    val stream: Stream[F, A] = for {
      c <- Stream.eval(controller.get)
      req = Sync[F].delay(c.request(chunkSize))
      items <- Stream
        .evalSeq(req *> queue.take.product(queue.tryTakeN(Some(chunkSize - 1))).map { case (hd, tl) =>
          hd +: tl
        })
        .repeat
        .unNoneTerminate
        .interruptWhen(err.get.map(_.asLeft[Unit]))
    } yield items

    def cancel: F[Unit] = controller.get.flatMap(c => Sync[F].delay(c.cancel()))

    override def onStart(c: StreamController): Unit = {
      c.disableAutoInboundFlowControl()
      dispatcher.unsafeRunAndForget(controller.complete(c))
    }

    override def onResponse(response: A): Unit = dispatcher.unsafeRunAndForget(queue.offer(Some(response)))

    override def onError(t: Throwable): Unit = dispatcher.unsafeRunAndForget(err.complete(t))

    override def onComplete(): Unit = dispatcher.unsafeRunAndForget(queue.offer(None))

  }
}
