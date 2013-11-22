package com.pragmasoft.scaldingunit.sample.unit

import com.pragmasoft.scaldingunit.sample.{schemas, SampleJobPipeTransformations}
import SampleJobPipeTransformations._
import com.twitter.scalding.{TupleConversions, RichPipe}
import scala.collection.mutable
import schemas._
import org.specs2.{mutable => mutableSpec}
import com.pragmasoft.scaldingunit.TestInfrastructure


class SampleJobPipeTransformationsSpec2Spec extends mutableSpec.SpecificationWithJUnit with TupleConversions with TestInfrastructure {

  // See: https://github.com/twitter/scalding/wiki/Frequently-asked-questions

  "A sample job pipe transformation" should {

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

    Given {
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
    } When {
      pipe: RichPipe => pipe.countUserEventsPerDay
    } Then {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        "count user events per day" in {
          def lessThanByDateAndId(left: (String, Long, Long), right: (String, Long, Long)): Boolean =
            (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2))


          buffer.toList.sortWith(lessThanByDateAndId(_, _)) shouldEqual List(
            ("2013/02/11", 1000002l, 1l),
            ("2013/02/12", 1000002l, 2l),
            ("2013/02/15", 1000001l, 2l),
            ("2013/02/15", 1000002l, 2l),
            ("2013/02/15", 1000003l, 1l)
          )
        }
    }


    Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } And {
      List((1000002l, "stefano@email.com", "10 Downing St. London")) withSchema USER_DATA_SCHEMA
    } When {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        "add user info" in {
          buffer.toList shouldEqual List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l))
        }
    }

  }
}
