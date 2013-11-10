package com.pragmasoft.scaldingunit.sample.unit

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.pragmasoft.scaldingunit.TestInfrastructure
import com.pragmasoft.scaldingunit.sample.{schemas, SampleJobPipeTransformations}
import SampleJobPipeTransformations._
import com.twitter.scalding.{TupleConversions, RichPipe}
import scala.collection.mutable
import cascading.tuple.Tuple
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import schemas._


@RunWith(classOf[JUnitRunner])
class SampleJobPipeTransformationsSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  "A sample job pipe transformation" should "add column with day of event" in {
    given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema INPUT_SCHEMA
    } when {
      pipe: RichPipe => pipe.addDayColumn
    } ensure {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
    }
  }

  it should "count user events per day" in {
    def lessThanByDateAndId(left: (String, Long, Long), right: (String, Long, Long)): Boolean =
      (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2))

    given {
      List(
        ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"),
        ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"),
        ("11/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/11"),
        ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000001l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000003l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000001l, "http://www.youtube.com", "2013/02/15"),
        ("15/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/15")
      ) withSchema WITH_DAY_SCHEMA
    } when {
      pipe: RichPipe => pipe.countUserEventsPerDay
    } ensure {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        buffer.toList.sortWith(lessThanByDateAndId(_, _)) shouldEqual List(
          ("2013/02/11", 1000002l, 1l),
          ("2013/02/12", 1000002l, 2l),
          ("2013/02/15", 1000001l, 2l),
          ("2013/02/15", 1000002l, 2l),
          ("2013/02/15", 1000003l, 1l)
        )
    }
  }

  it should "add user info" in {
    given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } and {
      List((1000002l, "stefano@email.com", "10 Downing St. London")) withSchema USER_DATA_SCHEMA
    } when {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } ensure {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
    }
  }
}
