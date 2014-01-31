package com.pragmasoft.scaldingunit

import com.twitter.scalding._
import cascading.tuple.Tuple
import scala.Predef._
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable.Buffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import cascading.pipe.Pipe
import Dsl._


// Should stay here and not inside the class otherwise Hadoop will try to serialise the container too
object ScalaTestHadoopSupportSpecOperations {
  implicit class OperationsWrapper(val pipe: Pipe) extends Serializable {
    def changeColValue : Pipe = {
      pipe.map('col1 -> 'col1_transf) {
        col1: String => col1 + "_transf"
      }
    }

    def withTwoPipes(pipe2: Pipe) : Pipe = {
      pipe.joinWithSmaller('name -> 'name, pipe2).map('address -> 'address_transf) {
        address: String => address + "_transf"
      }
    }
  }
  implicit def fromRichPipe(rp: RichPipe) = new OperationsWrapper(rp.pipe)
}

@RunWith(classOf[JUnitRunner])
class ScalaTestHadoopSupportSpec extends FlatSpec with Matchers {
  import ScalaTestHadoopSupportSpecOperations._

  "A test with single source" should "accept an operation with a single input rich pipe" in new HadoopTestInfrastrucutureWithSpy {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: RichPipe => pipe.changeColValue
    } Then {
      buffer: Buffer[(String, String, String)] => {
        buffer.forall({
          case (_, _, transformed) => transformed.endsWith("_transf")
        }) should be(true)
      }
    }

    assert( testHasBeenExecutedInHadoopMode )
  }


  "A test with two sources" should "accept an operation with two input richPipes" in new HadoopTestInfrastrucutureWithSpy {
    Given {
      List(("Stefano", "110"), ("Rajah", "220")) withSchema('name, 'points)
    } And {
      List(("Stefano", "home1"), ("Rajah", "home2")) withSchema('name, 'address)
    } When {
      (pipe1: RichPipe, pipe2: RichPipe) => pipe1.withTwoPipes(pipe2)
    } Then {
      buffer: Buffer[(String, String, String, String)] => {
        println("Output " + buffer.toList)
        buffer.forall({
          case (_, _, _, addressTransf) => addressTransf.endsWith("_transf")
        }) should be(true)
      }
    }

    assert( testHasBeenExecutedInHadoopMode )
  }
}