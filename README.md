# ScaldingUnit - TDD utils for Scalding developers

[![Build Status](https://api.travis-ci.org/galarragas/ScaldingUnit.png)](http://travis-ci.org/galarragas/ScaldingUnit)

The aim of this project is to allow user to write Scalding (https://github.com/twitter/scalding) map-reduce jobs in a more modular and test-driven way.
It is based on the experience done in the Big Data unity at BSkyB where it originated and is currently used and maintained.
It essentially provides a test harness to support the decomposition of a Scalding Map-Reduce Job into a series of smaller steps,
each one testable independently before being assembled into the main Job that will then be tested as a whole using Scalding-based
tests.

## What does it look like

A test written with scalding unit look as shown below:

```scala
class SampleJobPipeTransformationsSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  "A sample job pipe transformation" should "add user info" in {
    given {
      List(("2013/02/11", 1000002l, 1l)) withSchema EVENT_COUNT_SCHEMA
    } and {
      List( (1000002l, "stefano@email.com", "10 Downing St. London") ) withSchema USER_DATA_SCHEMA
    } when {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } ensure {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
    }
  }
}
```

Where `addUserInfo` is a function joining two pipes to generate an enriched one.

## Motivation and details

## Writing and testing Scalding Jobs without ScaldingUnit

A Scalding job consists in a series of transformations applied to one or more sources in order to create one or more
output resources or sinks. A very simple example taken from the Scalding documentations is as follows.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
    TextLine( args("input") )
     .flatMap('line -> 'word) { line : String => tokenize(line) }
     .groupBy('word) { _.size }
     .write( Tsv( args("output") ) )

    // Split a piece of text into individual words.
    def tokenize(text : String) : Array[String] = {
     // Lowercase each word and remove punctuation.
     text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
    }
}
```

The transformations are defined as operations on a `cascading.pipe.Pipe` class or on the richer wrapper `com.twitter.scalding.RichPipe`.
Scalding provides a way of testing Jobs via the com.twitter.scalding.JobTest class. This class allows to specify values for the different
Job sources and to specify assertions on the different job sinks.
This approach works very well to do end to end test on the Job and is good enough for small jobs as the one described above
but doesn't encourage modularisation when writing more complex Jobs. That's the reason we started working on ScaldingUnit.

## Checking your pipes, modular unit test of your Scalding Job

When the Job logic become more complex it is very helpful to decompose its work in simpler functions to be tested independently before being
aggregated into the Job.

Let's consider the following Scalding Job (it is still a simple one for reason of space but should give the idea):

```scala
class SampleJob(args: Args) extends Job(args) {
  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  Osv(args("eventsPath")).read
    .map('date -> 'day) {
           date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN).parseDateTime(date).toString("yyyy/MM/dd");
         }
    .groupBy(('day, 'userid)) { _.size('event_count) }
    .joinWithLarger('userid -> 'userid, Osv(args("userInfoPath")).read).project(OUTPUT_SCHEMA)
    .write(Tsv(args("outputPath")))
}
```

It is possible to identify three main operation performed during this task. Each one is providing a identifiable and
 autonomous transformation to the pipe, just relying on a specific input schema and generating a transformed pipe with a
 potentially different output schema. Following this idea it is possible to write the Job in this way:

```scala
import SampleJobPipeTransformations._

class SampleJob(args: Args) extends Job(args) {

  Osv(args("eventsPath")).read
    .addDayColumn
    .countUserEventsPerDay
    .addUserInfo(Osv(args("userInfoPath")).read)
    .write(Tsv(args("outputPath")))
}
```

Where the single operations have been extracted as basic functions into a separate class that is not a Scalding Job:

```scala
package object SampleJobPipeTransformations  {

  val INPUT_SCHEMA = List('date, 'userid, 'url)
  val WITH_DAY_SCHEMA = List('date, 'userid, 'url, 'day)
  val EVENT_COUNT_SCHEMA = List('day, 'userid, 'event_count)
  val OUTPUT_SCHEMA = List('day, 'userid, 'email, 'address, 'event_count)

  val USER_DATA_SCHEMA = List('userid, 'email, 'address)

  val INPUT_DATE_PATTERN: String = "dd/MM/yyyy HH:mm:ss"

  implicit def wrapPipe(pipe: Pipe): SampleJobPipeTransformationsWrapper = new SampleJobPipeTransformationsWrapper(new RichPipe(pipe))
  implicit class SampleJobPipeTransformationsWrapper(val self: RichPipe) extends PipeOperations {

    /**
     * Input schema: INPUT_SCHEMA
     * Output schema: WITH_DAY_SCHEMA
     * @return
     */
    def addDayColumn = self.map('date -> 'day) {
      date: String => DateTimeFormat.forPattern(INPUT_DATE_PATTERN).parseDateTime(date).toString("yyyy/MM/dd");
    }

    /**
     * Input schema: WITH_DAY_SCHEMA
     * Output schema: EVENT_COUNT_SCHEMA
     * @return
     */
    def countUserEventsPerDay = self.groupBy(('day, 'userid)) { _.size('event_count) }

    /**
     * Joins with userData to add email and address
     *
     * Input schema: WITH_DAY_SCHEMA
     * User data schema: USER_DATA_SCHEMA
     * Output schema: OUTPUT_SCHEMA
     */
    def addUserInfo(userData: Pipe) = self.joinWithLarger('userid -> 'userid, userData).project(OUTPUT_SCHEMA)
  }
}
```

The main Job class is responsible of essentially dealing with the configuration, opening the input and output pipes and
combining those macro operations. Using implicit transformations and value classes as shown above is possible to use those
operations as if they were part of the RichPipe class.

It is now possible to test all this method independently and without caring about the source and sinks of the job and of
the way the configuration is given.

The specification of the transformation class is shown below:

```scala
class SampleJobPipeTransformationsSpec extends FlatSpec with ShouldMatchers with TupleConversions with TestInfrastructure {
  "A sample job pipe transformation" should "add column with day of event" in {
    given {
      List( ("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com") ) withSchema INPUT_SCHEMA
    } when {
      pipe: RichPipe => pipe.addDayColumn
    } ensure {
      buffer: mutable.Buffer[(String, Long, String, String)] =>
        buffer.toList(0) shouldEqual (("12/02/2013 10:22:11", 1000002l, "http://www.youtube.com", "2013/02/12"))
    }
  }

  it should "count user events per day" in {
    def lessThanByDateAndId( left: (String, Long, Long), right: (String, Long, Long)): Boolean =
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
      List( (1000002l, "stefano@email.com", "10 Downing St. London") ) withSchema USER_DATA_SCHEMA
    } when {
      (eventCount: RichPipe, userData: RichPipe) => eventCount.addUserInfo(userData)
    } ensure {
      buffer: mutable.Buffer[(String, Long, String, String, Long)] =>
        buffer.toList shouldEqual List( ("2013/02/11", 1000002l, "stefano@email.com", "10 Downing St. London", 1l) )
    }
  }
}
```

The TestInfrastructure trait is providing a BDD-like syntax to specify the Input to supply to the operation to test and
to write the expectations on the results (unfortunately we had to remove the `then` keyword since it is deprecated from Scala 2.10).

Once the different steps have been tested thoroughly it is possible to combine them in the main Job and test the end to end
behavior using the JobTest class provided by Scalding.

## Content

The repository contains two projects:

 - scalding-unit: the main project, providing the test framework
 - examples: a set of examples to describe the design approach described here

## What's next

At the moment we are covering only the Fields-based API since is the one used in most of the project we are working on.
We are planning to start providing a similar infrastructure for the type-safe API too.
We are also planning to add support for job acceptance validation comparing sample data process against an external
executable specification (such as a R program)

## License

The sw is distributed under apache license (see license document)


