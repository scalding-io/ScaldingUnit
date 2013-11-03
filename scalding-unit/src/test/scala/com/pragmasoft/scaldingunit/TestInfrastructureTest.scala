package com.pragmasoft.scaldingunit

import com.twitter.scalding._
import cascading.tuple.Tuple
import scala.Predef._
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection.mutable.Buffer
import com.pragmasoft.scaldingunit.TestInfrastructure


/**
 * Created with IntelliJ IDEA.
 * User: gfe01
 * Date: 21/10/13
 * Time: 11:14
 * To change this template use File | Settings | File Templates.
 */
class TestInfrastructureTest extends FlatSpec with ShouldMatchers with TestInfrastructure {

  "A test with single source" should "accept an operation with a single input pipe" in {
    given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } when {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } then {
      buffer : Buffer[(String, String, String)] => {
        buffer.forall( {case (_,_,transformed) => transformed.endsWith("_transf") } ) should be(true)
      }
    }
  }

  "A test with single source" should "work with output as Tuple" in {
    given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } when {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall( tuple => tuple.getString(2).endsWith("_transf") )  should be(true)
      }
    }
  }

  "A test with single source" should "work with input as simple type" in {
    given {
      List("col1_1", "col1_2") withSchema ('col1)
    } when {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall( tuple => tuple.getString(1).endsWith("_transf") )  should be(true)
      }
    }
  }

  "A test with single source" should "work with input as Tuple" in {
    given {
      List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema (('col1, 'col2))
    } when {
      pipe: RichPipe => {
        pipe.map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall( tuple => tuple.getString(2).endsWith("_transf") )  should be(true)
      }
    }
  }

  "A test with two sources" should "accept an operation with two input pipes" in {
    given {
      List(("Stefano", "110"), ("Rajah", "220")) withSchema('name, 'points)
    } and {
      List(("Stefano", "home1"), ("Rajah", "home2")) withSchema('name, 'address)
    } when {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1.joinWithSmaller( 'name -> 'name, pipe2 ).map('address -> 'address_transf) {
          address: String => address + "_transf"
        }
      }
    } then {
      buffer : Buffer[(String, String, String, String)] => {
        println("Output " + buffer.toList)
        buffer.forall( { case (_, _, _, addressTransf ) => addressTransf.endsWith("_transf") } ) should be(true)
      }
    }
  }

  "A test with two sources" should "accept an operation with two input pipes using Tuples" in {
    given {
      List( new Tuple("Stefano", "110"), new Tuple("Rajah", "220")) withSchema('name, 'points)
    } and {
      List( new Tuple("Stefano", "home1"), new Tuple("Rajah", "home2")) withSchema('name, 'address)
    } when {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1.joinWithSmaller( 'name -> 'name, pipe2 ).map('address -> 'address_transf) {
          address: String => address + "_transf"
        }
      }
    } then {
      buffer : Buffer[(String, String, String, String)] => {
        println("Output " + buffer.toList)
        buffer.forall( { case (_, _, _, addressTransf ) => addressTransf.endsWith("_transf") } ) should be(true)
      }
    }
  }

  "A test with three sources" should "accept an operation with three input pipes" in {
    given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
    } and {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)
    } and {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
    } when {
      (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) => {
        pipe1
          .joinWithSmaller('col1 -> 'col1, pipe2)
          .joinWithSmaller('col1 -> 'col1, pipe3)
          .map('col1 -> 'col1_transf) {
          col1: String => col1 + "_transf"
        }
        .project( ('col1, 'col2, 'col1_transf) )
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  "A test with four sources" should "compile mixing an operation with inconsistent number of input pipes but fail at runtime" in {
    intercept[IllegalArgumentException] {
      given {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
      } and {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)
      } and {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
      } and {
        List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col5)
      } when {
        (pipe1: RichPipe, pipe2: RichPipe, pipe3: RichPipe) => {
          pipe1
            .joinWithSmaller('col1 -> 'col1, pipe2)
            .joinWithSmaller('col1 -> 'col1, pipe3)
            .joinWithSmaller('col1 -> 'col1, pipe3)
            .map('col1 -> 'col1_transf) {
                col1: String => col1 + "_transf"
              }
        }
      } then {
        buffer : Buffer[Tuple] => {
          buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
        }
      }
    }
  }

  it should "be used with a function accepting a list of sources because there is no implicit for functions with more than three input pipes" in {
    given {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)
    } and {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4)
    } and {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col5)
    } and {
      List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col6)
    } when {
      (pipes: List[RichPipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .joinWithSmaller('col1 -> 'col1, pipes(2))
          .joinWithSmaller('col1 -> 'col1, pipes(3))
          .map('col1 -> 'col1_transf) {
              col1: String => col1 + "_transf"
            }
          .project( ('col1, 'col2, 'col1_transf) )
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  "A test with a list sources" should "compile mixing it with a multi pipe function but fail if not same cardinality between given and when clause" in {
    intercept[IllegalArgumentException] {
      given {
        List(
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3)),
          (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col4))
        )
      } when {
        (pipe1: RichPipe, pipe2: RichPipe) => {
          pipe1
            .joinWithSmaller('col1 -> 'col1, pipe2)
            .map('col1 -> 'col1_transf) {
              col1: String => col1 + "_transf"
            }
        }
      } then {
        buffer : Buffer[Tuple] => {
          buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
        }
      }
    }
  }

  it should "work properly with a multi pipe function with same cardinality" in {
    given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } when {
      (pipe1: RichPipe, pipe2: RichPipe) => {
        pipe1
          .joinWithSmaller('col1 -> 'col1, pipe2)
          .map('col1 -> 'col1_transf) {
            col1: String => col1 + "_transf"
          }
          .project( ('col1, 'col2, 'col1_transf) )
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  it should "work properly with a function accepting a list of pipes" in {
    given {
      List(
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col2)),
        (List(("col1_1", "col2_1"), ("col1_2", "col2_2")) withSchema('col1, 'col3))
      )
    } when {
      (pipes: List[RichPipe]) => {
        pipes(0)
          .joinWithSmaller('col1 -> 'col1, pipes(1))
          .map('col1 -> 'col1_transf) {
            col1: String => col1 + "_transf"
          }
          .project( ('col1, 'col2, 'col1_transf) )
      }
    } then {
      buffer : Buffer[Tuple] => {
        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
      }
    }
  }

  //  it should "THIS DOESN'T EVEN COMPILE" in {
  //    given {
  //      List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema ('col1, 'col2)
  //    } and {
  //      List(new Tuple("col1_1", "col2_1"), new Tuple("col1_2", "col2_2")) withSchema ('col1, 'col2)
  //    } when {
  //      pipe1: RichPipe => {
  //        pipe1.map('col1 -> 'col1_transf) {
  //          col1: String => col1 + "_transf"
  //        }
  //      }
  //    } then {
  //      buffer => {
  //        Console.println("Result: " + buffer.toSeq)
  //        buffer.forall(tuple => tuple.getString(2).endsWith("_transf")) should be(true)
  //      }
  //    }
  //  }

}