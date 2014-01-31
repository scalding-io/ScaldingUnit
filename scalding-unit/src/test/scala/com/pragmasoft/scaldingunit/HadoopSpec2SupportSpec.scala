package com.pragmasoft.scaldingunit

import com.twitter.scalding._
import cascading.tuple.Tuple
import scala.Predef._
import scala.collection.mutable.Buffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import cascading.pipe.Pipe
import org.specs2.{mutable => mutableSpec}
import Dsl._

// Should stay here and not inside the class otherwise Hadoop will try to serialise the container too
object HadoopSpec2SupportSpecOperations {
  implicit class OperationsWrapper(val pipe: Pipe) extends Serializable {
    def changeColValue : Pipe = {
      pipe.map('col1 -> 'col1_transf) {
        col1: String => col1 + "_transf"
      }
    }
  }
  implicit def fromRichPipe(rp: RichPipe) = new OperationsWrapper(rp.pipe)
}

class HadoopSpec2SupportSpec extends mutableSpec.SpecificationWithJUnit with TupleConversions with Serializable  {
  import HadoopSpec2SupportSpecOperations._

  // Can only do one test. If I try to execute the test inside an instance of HadoopTestInfrastrucutureWithSpy I have
  // serialization issues
  "A test with single source" should {
    new HadoopTestInfrastrucutureWithSpy {
        Given {
          List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
        } When {
          pipe: RichPipe => pipe.changeColValue
        } Then {
          buffer: Buffer[(String, String, String)] =>
            "run in hadoop mode" in {
              testHasBeenExecutedInHadoopMode
            }
        }
      }

    () //without this spec2 gets annoyed
  }
}