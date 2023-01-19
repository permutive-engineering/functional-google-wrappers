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

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite

import scala.concurrent.duration._

class TestingBatcherSpec extends CatsEffectSuite {

  test(
    "TestingBatcher should correctly record inflight requests with no parallelism"
  ) {
    TestingBatcher
      .unit[IO]()
      .use(batcher =>
        for {
          _ <- batcher.suspendedBatcher.run(())
          _ <- batcher.suspendedBatcher.run(())
          res <- batcher.getState
        } yield assertEquals(
          res,
          TestingBatcher.State(inflight = 0, maxInflight = 1)
        )
      )
  }

  test(
    "TestingBatcher should correctly record inflight requests with parallelism"
  ) {
    val nFibers = 10
    TestingBatcher
      .unit[IO]()
      .use(batcher =>
        for {
          _ <- List
            .fill(nFibers)(batcher.suspendedBatcher.run(()))
            .parTraverse(identity)
          res <- batcher.getState
        } yield assertEquals(
          res,
          TestingBatcher.State(inflight = 0, maxInflight = nFibers)
        )
      )
  }

  test("TestingBatcher should correctly record inflight requests with fibers") {
    val nFibers = 10

    TestingBatcher
      .unit[IO]()
      .use(batcher =>
        for {
          fibers <- List
            .fill(nFibers)(batcher.suspendedBatcher.run(()))
            .traverse(_.start)
          // ApiFutures will start running in background, give them a tiny tick to get going though.
          // The test fails if this is not done, I presume because nothing gets executed (i.e. fibers aren't running) until
          // the first tick occurs.
          _ <- IO.sleep(1.millisecond)
          inflight <- batcher.getState
          _ <- fibers.traverse_(_.join)
          end <- batcher.getState
        } yield {
          assertEquals(
            inflight,
            TestingBatcher.State(inflight = nFibers, maxInflight = nFibers)
          )
          assertEquals(
            end,
            TestingBatcher.State(inflight = 0, maxInflight = nFibers)
          )
        }
      )
  }
}
