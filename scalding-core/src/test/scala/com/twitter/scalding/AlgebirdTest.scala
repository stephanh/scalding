/*
 Copyright 2012 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.twitter.scalding

import com.twitter.algebird._

import org.specs._

import TDsl._

class CountMinSketchAggregatorJob(args: Args) extends Job(args) {
  TypedCsv[Long]("input")
    .aggregate(CMS.aggregator(1E-10, 0.001, 1, 0.1))
    .flatMap(_.heavyHitters)
    .write(TypedCsv("output"))

}

class CountMinSketchAggregatorTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A Typed Pipe using CountMinSketchAggregator" should {
    JobTest(new CountMinSketchAggregatorJob(_))
      .source(TypedCsv[Long]("input"), List(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L, 5L, 5L, 5L, 5L, 5L))
      .sink[Long](TypedCsv[Long]("output"))(outputBuffer => {
        "correctly calculate heavy hitters" in {
          outputBuffer.toList must be_==(List(2L, 3L, 4L, 5L))
        }
      })
      .runHadoop
      .finish
  }
}

class HyperLogLogAggregatorJob(args: Args) extends Job(args) {
  TypedCsv[Long]("input")
    .aggregate(HyperLogLogAggregator.sizeAggregator(12).composePrepare(HyperLogLog.long2Bytes(_)))
    .write(TypedCsv("output"))

}

class HyperLogLogAggregatorTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  import Dsl._
  "A Typed Pipe using HyperLogLogAggregator" should {
    JobTest(new HyperLogLogAggregatorJob(_))
      .source(TypedCsv[Long]("input"), List(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 4L, 4L, 5L, 5L, 5L, 5L, 5L))
      .sink[Double](TypedCsv[Double]("output"))(outputBuffer => {
        "correctly calculate size" in {
          outputBuffer.head must be_==(5.0)
        }
      })
      .runHadoop
      .finish
  }
}
