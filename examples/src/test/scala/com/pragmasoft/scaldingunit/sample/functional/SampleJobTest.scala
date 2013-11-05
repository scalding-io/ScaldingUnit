package com.pragmasoft.scaldingunit.sample.functional

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import com.twitter.scalding._
import cascading.tuple.Tuple
import com.pragmasoft.scaldingunit.sample.SampleJob
import com.pragmasoft.scaldingunit.sample.SampleJobPipeTransformations._
import scala.collection.mutable
import com.twitter.scalding.Tsv
import com.twitter.scalding.Osv

class SampleJobTest extends FlatSpec with ShouldMatchers with FieldConversions with TupleConversions {

  val eventData = List( ("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com") )
  val userData = List( (1000002l, "stefano@email.com", "10 Downing St. London") )

  "A sample job" should "do the full transformation" in {


    JobTest(classOf[SampleJob].getName)
      .arg("eventsPath", "eventsPath")
      .arg("userInfoPath", "userInfoPath")
      .arg("outputPath", "outputPath")
      .source(Osv("eventsPath", INPUT_SCHEMA), eventData)
      .source(Osv("userInfoPath", USER_DATA_SCHEMA), userData)
      .sink(Tsv("outputPath", OUTPUT_SCHEMA)) {
            buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
              buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
          }
      .run
      .finish
  }
}
