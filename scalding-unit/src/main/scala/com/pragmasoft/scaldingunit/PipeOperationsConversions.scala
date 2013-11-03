package com.pragmasoft.scaldingunit

import com.twitter.scalding.RichPipe
import cascading.pipe.Pipe
import scala.language.implicitConversions

trait PipeOperationsConversions extends PipeOperations {
  trait PipeOperation {
    def assertPipeSize(pipes: List[RichPipe], expectedSize: Int) =
      require(pipes.size == expectedSize, s"Cannot apply an operation for $expectedSize pipes to ${pipes.size} pipes. " +
        s"Verify matching of given and when clauses in test case definition")

    def apply(pipes: List[RichPipe]): Pipe
  }

  class OnePipeOperation(op: RichPipe => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 1); op(pipes(0))
    }
  }

  class TwoPipesOperation(op: (RichPipe, Pipe) => RichPipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 2); op(pipes(0), pipes(1))
    }
  }

  class ThreePipesOperation(op: (RichPipe, RichPipe, RichPipe) => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 3); op(pipes(0), pipes(1), pipes(2))
    }
  }

  class ListPipesOperation(op: List[RichPipe] => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = op(pipes)
  }

  implicit val fromSinglePipeFunctionToOperation = (op: RichPipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSingleRichPipeToPipeFunctionToOperation = (op: RichPipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoPipeFunctionToOperation = (op: (RichPipe, RichPipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipeToPipeFunctionToOperation = (op: (RichPipe, RichPipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreePipeFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipeToPipeFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromListPipeFunctionToOperation = (op: List[RichPipe] => RichPipe) => new ListPipesOperation(op(_).pipe)
  implicit val fromListRichPipeToPipeFunctionToOperation = (op: List[RichPipe] => Pipe) => new ListPipesOperation(op(_))
}
