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

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.{Deferred, Resource}
import cats.syntax.all._
import cats.effect.testkit.TestControl
import munit.{CatsEffectSuite, Location, ScalaCheckEffectSuite}
import org.scalacheck.effect.PropF._

import scala.concurrent.duration._

class KleisliBatcherSpec extends CatsEffectSuite with ScalaCheckEffectSuite {

  private def resources[A, B](
      permits: Int,
      result: A => IO[B]
  ): Resource[IO, (TestingBatcher[IO, A, B], Kleisli[IO, A, IO[B]])] =
    for {
      tb <- TestingBatcher.resource[IO, A, B](result)
      sut <- FunctionalGax.fromSuspendedBatcher(tb.suspendedBatcher, permits)
    } yield (tb, sut)

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should pass any input to the underlying suspended batcher"
  ) {
    val mirrorResources = resources[Int, Int](1, _.pure[IO])

    forAllF { (i: Int) =>
      TestControl.executeEmbed(mirrorResources.use { case (_, sut) =>
        assertIO(sut.run(i).flatten, i)
      })
    }
  }

  private def checkConcurrentAccess(
      permits: Int
  )(implicit loc: Location): IO[Unit] =
    TestControl.executeEmbed(resources[Unit, Unit](permits, _ => IO.unit).use { case (tb, sut) =>
      for {
        _ <- List
          .fill(permits * 10)(sut.run(()).flatten)
          .parTraverse_(identity)
        res <- tb.getState
      } yield assertEquals(
        res,
        TestingBatcher.State(inflight = 0, maxInflight = permits)
      )
    })

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should limit concurrent access to the underlying suspended batcher"
  ) {
    for {
      _ <- checkConcurrentAccess(1)
      _ <- checkConcurrentAccess(2)
      _ <- checkConcurrentAccess(5)
      _ <- checkConcurrentAccess(10)
    } yield ()
  }

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should return the final result immediately if both effects are flattened"
  ) {
    TestControl.executeEmbed {
      val expectedRes = 100

      val resourcesInnerResultSleep =
        resources[Unit, Int](1, _ => IO.sleep(5.seconds).as(expectedRes))

      resourcesInnerResultSleep.use { case (tb, sut) =>
        for {
          _ <- assertIO(sut.run(()).flatten, expectedRes)
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
        } yield ()
      }
    }
  }

  val resourcesInnerSleepCompleteDeferred =
    for {
      deferred <- Resource.eval(Deferred[IO, Unit])
      res <- resources[Unit, Unit](
        1,
        _ => IO.sleep(5.seconds) >> deferred.complete(()).void
      )
    } yield (deferred, res._1, res._2)

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should not block waiting for inner effect to complete (evaluating inner effect)"
  ) {
    TestControl.executeEmbed {
      resourcesInnerSleepCompleteDeferred.use { case (deferred, tb, sut) =>
        for {
          inner <- sut.run(())
          // Check that the outer effect has run
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
          // Check that inner effect has not completed
          _ <- assertIO(deferred.tryGet, None)
          // Evaluate inner effect to await completion
          _ <- inner
          _ <- assertIO(deferred.tryGet, Some(()))
        } yield ()
      }
    }
  }

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should not block waiting for inner effect to complete (starting and joining inner effect)"
  ) {
    TestControl.executeEmbed {
      resourcesInnerSleepCompleteDeferred.use { case (deferred, tb, sut) =>
        for {
          inner <- sut.run(())
          // Check that the outer effect has run
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
          fiber <- inner.start
          // Check that inner effect has not completed
          _ <- assertIO(deferred.tryGet, None)
          // Join inner fiber to await completion
          _ <- fiber.join
          _ <- assertIO(deferred.tryGet, Some(()))
        } yield ()
      }
    }
  }

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should not block waiting for inner effect to complete (starting and never joining inner effect)"
  ) {
    TestControl.executeEmbed {
      resourcesInnerSleepCompleteDeferred.use { case (deferred, tb, sut) =>
        for {
          inner <- sut.run(())
          // Check that the outer effect has run
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
          _ <- inner.start
          // Check that inner effect has not completed
          _ <- assertIO(deferred.tryGet, None)
          // Wait until the inner effect should have completed and check again
          _ <- IO.sleep(10.seconds)
          _ <- assertIO(deferred.tryGet, Some(()))
        } yield ()
      }
    }
  }

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should not block waiting for inner effect to complete (never starting inner effect)"
  ) {
    TestControl.executeEmbed {
      // I think this is technically impure/buggy behaviour and is a result of us having to interoperate with `ApiFuture`;
      // we have to call `unsafeToApiFuture` in the `TestingBatcher`.
      // If this were referentially transparent I _think_ that the deferred would never complete in this test because the
      // inner effect would never run.
      // Fundamentally what we are doing is interoperating with something impure/not referentially transparent though so
      // this is OK/expected!
      resourcesInnerSleepCompleteDeferred.use { case (deferred, tb, sut) =>
        for {
          _ <- sut.run(())
          // Check that the outer effect has run
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
          // Check that inner effect has not completed
          _ <- assertIO(deferred.tryGet, None)
          // Wait until the inner effect should have completed and check again
          _ <- IO.sleep(10.seconds)
          _ <- assertIO(deferred.tryGet, Some(()))
        } yield ()
      }
    }
  }

  case class CustomException(message: String) extends RuntimeException(message)

  test(
    "FunctionalGax.fromSuspendedBatcher(_).run should not fail the outer effect if the inner effect fails"
  ) {
    TestControl.executeEmbed {
      val errorMessage = "Boom!"

      val resourcesInnerFailure =
        resources[Unit, Unit](
          1,
          _ => IO.raiseError(CustomException(errorMessage))
        )

      resourcesInnerFailure.use { case (tb, sut) =>
        for {
          inner <- sut.run(())
          _ <- assertIO(
            tb.getState,
            TestingBatcher.State(inflight = 0, maxInflight = 1)
          )
          _ <- interceptMessageIO[CustomException](errorMessage)(inner)
        } yield ()
      }
    }
  }
}
