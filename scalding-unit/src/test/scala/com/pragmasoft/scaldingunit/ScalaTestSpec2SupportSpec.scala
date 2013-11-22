package com.pragmasoft.scaldingunit

import com.twitter.scalding._
import scala.Predef._
import scala.collection.mutable.Buffer
import org.specs2.{mutable => mutableSpec}

class ScalaTestSpec2SupportSpec extends mutableSpec.SpecificationWithJUnit with TestInfrastructure {

  // See: https://github.com/twitter/scalding/wiki/Frequently-asked-questions

  "A test with single source" should {

    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[(String, String, String)] =>
        "accept an operation with a single input pipe" in { buffer.forall({ case (_, _, transformed) => transformed.endsWith("_transf")}) mustEqual (true) }
    }
  }
}