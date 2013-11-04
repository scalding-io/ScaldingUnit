package com.pragmasoft.scaldingunit.sample.functional

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import com.twitter.scalding.{Tsv, Osv, JobTest}
import cascading.tuple.Tuple
import com.pragmasoft.scaldingunit.sample.SampleJob

class SampleJobTest extends FlatSpec with ShouldMatchers {

  "A sample job" should "do the full transformation" in {

    JobTest(classOf[SampleJob].getName)
      .arg("eventsPath", "eventsPath")
      .arg("userInfoPath", "userInfoPath")
      .arg("outputPath", "outputPath")
      .source[Tuple](Osv("eventsPath", MPOD_INPUT_SCHEMA), mpodData)
      .source[Tuple](Osv("userInfoPath"), registeredUserData)
      .sink[Tuple](Tsv("outputPath", MPOD_AMENDED_ID_SCHEMA))(assertion)
      .run
      .finish
  }
}
