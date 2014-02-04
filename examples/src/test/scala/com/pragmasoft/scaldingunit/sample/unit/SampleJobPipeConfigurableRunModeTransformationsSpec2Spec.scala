package com.pragmasoft.scaldingunit.sample.unit

import com.pragmasoft.scaldingunit.sample.{schemas, SampleJobPipeTransformations}
import SampleJobPipeTransformations._
import com.twitter.scalding.{TupleConversions, RichPipe}
import scala.collection.mutable
import schemas._
import org.specs2.{mutable => mutableSpec}
import com.pragmasoft.scaldingunit.{MultiTestModeTestInfrastructure, HadoopTestInfrastructure}
import com.twitter.scalding.Dsl._
import com.pragmasoft.scaldingunit.testmode.TestRunMode

class SampleJobPipeConfigurableRunModeTransformationsSpec2Spec extends mutableSpec.SpecificationWithJUnit with TupleConversions with MultiTestModeTestInfrastructure {

  // See: https://github.com/twitter/scalding/wiki/Frequently-asked-questions

  import TestRunMode._

  "You can run on hadoop" should {

    implicit val _ = testOnHadoop

    Given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema INPUT_SCHEMA
    } When {
      pipe: RichPipe => pipe.addDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        "add column with day of event" in {
          buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
        }
    }
  }

  "You can run locally" should {

    implicit val _ = testLocally

    Given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema INPUT_SCHEMA
    } When {
      pipe: RichPipe => pipe.addDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        "add column with day of event" in {
          buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
        }
    }
  }

  "You can run decide using some configuration parameter or randomly" should {

    implicit val _ = if( (System.currentTimeMillis % 2) == 0) testLocally else testOnHadoop

    Given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema INPUT_SCHEMA
    } When {
      pipe: RichPipe => pipe.addDayColumn
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        "add column with day of event" in {
          buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
        }
    }
  }


}
