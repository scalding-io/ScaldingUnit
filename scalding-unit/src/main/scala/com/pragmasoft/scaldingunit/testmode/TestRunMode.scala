package com.pragmasoft.scaldingunit.testmode

import com.twitter.scalding.JobTest

trait TestRunMode {
  def runJobTest(jobTest: JobTest) : JobTest
}


object TestRunMode {
  val testLocally = new TestRunMode {
    def runJobTest(jobTest: JobTest): JobTest = jobTest.run
  }

  val testOnHadoop =  new TestRunMode with Serializable {
    def runJobTest(jobTest: JobTest): JobTest = jobTest.runHadoop
  }
}

import TestRunMode._
trait WithLocalTest extends WithTestMode {
  implicit val testMode = testLocally

  /**
   * THis is only used to unit test the test infrastructure itself
   */
  implicit val __jobTestSpy : Option[JobTestSpy] = None
}

trait BaseHadoopTest extends WithTestMode {
  implicit val testMode = testOnHadoop
}

trait WithHadoopTest extends BaseHadoopTest {
  implicit val __jobTestSpy : Option[JobTestSpy] = None
}

trait WithHadoopTestSpyingJobExecution extends BaseHadoopTest {
  implicit val __jobTestSpy : Option[JobTestSpy] = Some(JobTestSpy())

  def testHasBeenExecutedInHadoopMode : Boolean = __jobTestSpy.get.hasBeenRunInHadoopMode.get
}

