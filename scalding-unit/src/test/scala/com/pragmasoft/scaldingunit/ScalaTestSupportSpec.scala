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

@RunWith(classOf[JUnitRunner])
class ScalaTestSupportSpec extends FlatSpec with Matchers with TestInfrastructure {


  "A test with single source" should "accept an operation with a single input rich pipe" in {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[(String, String, String)] => {
        buffer.forall({
          case (_, _, transformed) => transformed.endsWith("_transf")
        }) should be(true)
      }
    }
  }

  it should "accept an operation with a single input pipe" in {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: Pipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[(String, String, String)] => {
        buffer.forall({
          case (_, _, transformed) => transformed.endsWith("_transf")
        }) should be(true)
      }
    }
  }

  it should "work with output as Tuple" in {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work with input as simple type" in {
    Given {
      List("col1_1", "col1_2") withSchema ('col1)
    } When {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(1).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work with input as Tuple" in {
    Given {
      List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } When {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  "A test with two sources" should "accept an operation with two input richPipes" in {
    Given {
      List(("Stefano", "110"), ("Rajah", "220")) withSchema('name, 'points)
    } And {
      List(("Stefano", "home1"), ("Rajah", "home2")) withSchema('name, 'address)
    } When {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1.joinWithSmaller('name -> 'name, pipe2).map('address -> 'address_transf) {
          address: String => address + "_transf"
        }
      }
    } Then {
      buffer: Buffer[(String, String, String, String)] => {
        println("Output " + buffer.toList)
        buffer.forall({
          case (_, _, _, addressTransf) => addressTransf.endsWith("_transf")
        }) should be(true)
      }
    }
  }

  it should "accept an operation with two input richPipes using Tuples" in {
    Given {
      List(new Tuple("Stefano", "110"), new Tuple("Rajah", "220")) withSchema('name, 'points)
    } And {
      List(new Tuple("Stefano", "home1"), new Tuple("Rajah", "home2")) withSchema('name, 'address)
    } When {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1.joinWithSmaller('name -> 'name, pipe2).map('address -> 'address_transf) {
          address: String => address + "_transf"
        }
      }
    } Then {
      buffer: Buffer[(String, String, String, String)] => {
        println("Output " + buffer.toList)
        buffer.forall({
          case (_, _, _, addressTransf) => addressTransf.endsWith("_transf")
        }) should be(true)
      }
    }
  }

  "A test with three sources" should "accept an operation with three input richPipes" in {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
    } And {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)
    } And {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
    } When {
      (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) => {
        pipe1
          .joinWithSmaller('col1 -> 'col1, pipe2)
          .joinWithSmaller('col1 -> 'col1, pipe3)
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  "A test with four sources" should "compile mixing an operation with inconsistent number of input richPipes but fail at runtime" in {
    intercept[IllegalArgumentException] {
      Given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
      } And {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col5)
      } When {
        (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) => {
          pipe1
            .joinWithSmaller('col1 -> 'col1, pipe2)
            .joinWithSmaller('col1 -> 'col1, pipe3)
            .joinWithSmaller('col1 -> 'col1, pipe3)
            .map('col1 -> 'col1_transf) {
            col1: String => col1 + "_transf"
          }
        }
      } Then {
        buffer: Buffer[Tuple] => {
          buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
        }
      }
    }
  }

  it should "be used with a function accepting a list of sources because there is no implicit for functions with more than three input richPipes" in {
    Given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
    } And {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
    } And {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col5)
    } And {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col6)
    } When {
      (pipes: List[RichPipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .joinWithSmaller('col1 -> 'col1, pipes(2))
          .joinWithSmaller('col1 -> 'col1, pipes(3))
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  "A test with a list sources" should "compile mixing it with a multi pipe function but fail if not same cardinality between given and when clause" in {
    intercept[IllegalArgumentException] {
      Given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4))
        )
      } When {
        (pipe1: RichPipe, pipe2: RichPipe) => {
          pipe1
            .joinWithSmaller('col1 -> 'col1, pipe2)
            .map('col1 -> 'col1_transf) {
            col1: String => col1 + "_transf"
          }
        }
      } Then {
        buffer: Buffer[Tuple] => {
          buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
        }
      }
    }
  }

  it should "work properly with a multi rich-pipe function with same cardinality" in {
    Given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } When {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1
          .joinWithSmaller('col1 -> 'col1, pipe2)
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work properly with a multi pipe function with same cardinality" in {
    Given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } When {
      (pipe1: Pipe, pipe2: Pipe) => {
        pipe1
          .joinWithSmaller('col1 -> 'col1, pipe2)
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work properly with a function accepting a list of rich richPipes" in {
    Given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } When {
      (pipes: List[RichPipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work properly with a function accepting a list of richPipes" in {
    Given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } When {
      (pipes: List[Pipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "support specification of onHadoop mode" in {
    Given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } When {
      (pipes: List[Pipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
          .project(('col1, 'col2, 'col1_transf))
      }
    } Then {
      buffer: Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

    it should "not compile if the number of sources is not the same of the test function in When" in {
      """Given {
        List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema ('col1, 'col2)
      } And {
        List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema ('col1, 'col2)
      } When {
        pipe1: RichPipe => {
          pipe1.map('col1 -> 'col1_transf) {
            col1: String => col1 + "_transf"
          }
        }
      } Then {
        buffer: Buffer[Tuple]  => {
          Console.println("Result: " + buffer.toSeq)
          buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
        }
      }""" shouldNot compile
    }

}