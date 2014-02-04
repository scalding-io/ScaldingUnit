package com.pragmasoft.scaldingunit

import com.twitter.scalding._
import scala.collection.mutable.Buffer
import cascading.tuple.Fields
import scala.Predef._
import com.twitter.scalding.Tsv
import org.slf4j.LoggerFactory
import scala.language.implicitConversions
import com.pragmasoft.scaldingunit.testmode._

abstract trait BaseTestInfrastructure extends FieldConversions with TupleConversions with PipeOperationsConversions {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  def Given(source: TestSource): TestCaseGiven1 = new TestCaseGiven1(source)

  def Given(sources: List[TestSource]): TestCaseGivenList = new TestCaseGivenList(sources)

  trait TestSourceWithoutSchema {
    def addSourceToJob(jobTest: JobTest, source: Source): JobTest

    def withSchema(schema: Fields) = new TestSource(this, schema)
  }


  class ProductTestSourceWithoutSchema(val data: Iterable[Product]) extends TestSourceWithoutSchema {
    def addSourceToJob(jobTest: JobTest, source: Source): JobTest = jobTest.source(source, data)
  }

  class SimpleTypeTestSourceWithoutSchema[T](val data: Iterable[T])(implicit setter: TupleSetter[T]) extends TestSourceWithoutSchema {
    println("Setter " + setter.getClass.getName)

    def addSourceToJob(jobTest: JobTest, source: Source): JobTest =
      jobTest.source[T](source, data)(setter)
  }

  implicit def fromProductDataToSourceWithoutSchema(data: Iterable[Product]) = new ProductTestSourceWithoutSchema(data)

  implicit def fromSimpleTypeDataToSourceWithoutSchema[T](data: Iterable[T])(implicit setter: TupleSetter[T]) =
    new SimpleTypeTestSourceWithoutSchema(data)(setter)

  class TestSource(data: TestSourceWithoutSchema, schema: Fields) {
    def sources = List(this)

    def name: String = "Source_" + hashCode

    def asSource: Source = Tsv(name, schema)

    def addSourceDataToJobTest(jobTest: JobTest) = data.addSourceToJob(jobTest, asSource)
  }

  case class TestCaseGiven1(source: TestSource) {
    def And(other: TestSource) = TestCaseGiven2(source, other)

    def When(op: OnePipeOperation): TestCaseWhen = TestCaseWhen(List(source), op)
  }

  case class TestCaseGiven2(source: TestSource, other: TestSource) {
    def And(third: TestSource) = TestCaseGiven3(source, other, third)

    def When(op: TwoPipesOperation): TestCaseWhen = TestCaseWhen(List(source, other), op)
  }

  case class TestCaseGiven3(source: TestSource, other: TestSource, third: TestSource) {
    def And(next: TestSource) = TestCaseGivenList(List(source, other, third, next))

    def When(op: ThreePipesOperation): TestCaseWhen = TestCaseWhen(List(source, other, third), op)
  }

  case class TestCaseGivenList(sources: List[TestSource]) {
    def And(next: TestSource) = TestCaseGivenList((next :: sources.reverse).reverse)

    def When(op: PipeOperation): TestCaseWhen = TestCaseWhen(sources, op)
  }


  case class TestCaseWhen(sources: List[TestSource], operation: PipeOperation) {
    def Then[OutputType](assertion: Buffer[OutputType] => Unit)(implicit conv: TupleConverter[OutputType], testRunMode : TestRunMode, spy: Option[JobTestSpy]) : Unit = {
      new CompleteTestCase(sources, operation, assertion, testRunMode, spy).run()
    }
  }

  class CompleteTestCase[OutputType](sources: List[TestSource], operation: PipeOperation, assertion: Buffer[OutputType] => Unit,
        jobRunMode: TestRunMode, jobSpy : => Option[JobTestSpy] )(implicit conv: TupleConverter[OutputType]) {

    class DummyJob(args: Args) extends Job(args) {
      val inputPipes: Seq[RichPipe] = sources map { testSource : TestSource => RichPipe(testSource.asSource.read) }

      val outputPipe = operation(inputPipes)

      outputPipe.write(Tsv("output"))
    }

    def run() : Unit = {
      val jobTest = JobTest(new DummyJob(_))

      val job = jobSpy match {
        case None => jobTest
        case Some(spy) =>
          spy.setDelegate(jobTest)
          spy
      }

      // Add Sources
      val op = sources.foreach {
        _.addSourceDataToJobTest(job)
      }
      // Add Sink
      job.sink[OutputType](Tsv("output")) {
        assertion(_)
      }

      // Execute
      jobRunMode.runJobTest(job).finish
    }
  }

}

trait MultiTestModeTestInfrastructure extends BaseTestInfrastructure {
  /**
   * THis is only used to unit test the test infrastructure itself
   */
  implicit val __jobTestSpy : Option[JobTestSpy] = None
}

trait TestInfrastructure extends BaseTestInfrastructure with WithLocalTest

trait HadoopTestInfrastructure extends BaseTestInfrastructure with WithHadoopTest

trait HadoopTestInfrastructureWithSpy extends BaseTestInfrastructure with WithHadoopTestSpyingJobExecution