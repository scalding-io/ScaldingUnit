package com.pragmasoft.scaldingunit.sample.unit

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.pragmasoft.scaldingunit.sample.{schemas, SampleJobPipeTransformations}
import com.twitter.scalding.{TupleConversions, RichPipe}
import scala.collection.mutable
import org.scalatest.junit.JUnitRunner
import schemas._
import com.pragmasoft.scaldingunit.TestInfrastructure
import cascading.pipe.Pipe
import org.scalatest.matchers._
import org.junit.runner.RunWith


/**
 * Testing the trait directly can be useful if you need to test some class extending a trait with some 
 * dependency to inject.
 *
 * No injection in this test but the mechanism is easily extensible
 */
@RunWith(classOf[JUnitRunner])
class SampleJobPipeTransformationsTraitScalaTestSpec extends FlatSpec with ShouldMatchers with TupleConversions {

  //We are not importing the 
  trait MyOperationExtension extends SampleJobPipeTransformations with TestInfrastructure {

  }

  "A sample job pipe transformation" should "add column with day of event" in new MyOperationExtension {
    Given {
      List(("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com")) withSchema INPUT_SCHEMA
    } When {
      pipe: Pipe => addDayColumn(pipe)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) should equal (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
    }
  }

  val byDateAndIdAsc =  (left: (String, Long, Long), right: (String, Long, Long)) => (left._1 < right._1) || ((left._1 == right._1) && (left._2 < left._2))

  it should "count user events per day" in new MyOperationExtension {
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
      pipe: Pipe => countUserEventsPerDay(pipe)
    } Then {
      buffer: mutable.Buffer[(String, Long, Long)] =>
        buffer.toList.sortWith(byDateAndIdAsc) should equal (List(
          ("2013/02/11", 1000002l, 1l),
          ("2013/02/12", 1000002l, 2l),
          ("2013/02/15", 1000001l, 2l),
          ("2013/02/15", 1000002l, 2l),
          ("2013/02/15", 1000003l, 1l)
        )) 
    }
  }

  it should "add user info" in new MyOperationExtension {
    Given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } And {
      List((1000002l, "stefano@email.com", "10 Downing St. London")) withSchema USER_DATA_SCHEMA
    } When {
      (eventCount: Pipe, userData: Pipe) => addUserInfo(eventCount, userData)
    } Then {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList should equal (List(("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l)))
    }
  }
}
