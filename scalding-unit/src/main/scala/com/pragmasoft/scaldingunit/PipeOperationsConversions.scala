package com.pragmasoft.scaldingunit

import com.twitter.scalding.{Dsl, RichPipe}
import cascading.pipe.Pipe
import scala.language.implicitConversions

trait PipeOperation {
  def assertPipeSize(pipes: Seq[RichPipe], expectedSize: Int) =
    require(pipes.size == expectedSize, s"Cannot apply an operation for $expectedSize richPipes to ${pipes.size} richPipes. " +
      s"Verify matching of given and when clauses in test case definition")

  def apply(pipes: Seq[RichPipe]): Pipe
}

class OnePipeOperation(op: RichPipe => Pipe) extends PipeOperation {
  def apply(pipes: Seq[RichPipe]): Pipe = {
    assertPipeSize(pipes, 1); op(pipes(0))
  }
}

class TwoPipesOperation(op: (RichPipe, RichPipe) => Pipe) extends PipeOperation {
  def apply(pipes: Seq[RichPipe]): Pipe = {
    assertPipeSize(pipes, 2); op(pipes(0), pipes(1))
  }
}

class ThreePipesOperation(op: (RichPipe, RichPipe, RichPipe) => Pipe) extends PipeOperation {
  def apply(pipes: Seq[RichPipe]): Pipe = {
    assertPipeSize(pipes, 3); op(pipes(0), pipes(1), pipes(2))
  }
}

class ListRichPipesOperation(op: Seq[RichPipe] => Pipe) extends PipeOperation {
  def apply(pipes: Seq[RichPipe]): Pipe = op(pipes)
}

class ListPipesOperation(op: Seq[Pipe] => Pipe) extends PipeOperation {
  def apply(richPipes: Seq[RichPipe]): Pipe = op( richPipes map { richPipe : RichPipe => richPipe.pipe } toSeq )
}

trait PipeOperationsConversions {

  implicit val fromSingleRichPipeFunctionToOperation = (op: RichPipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSingleRichPipeToPipeFunctionToOperation = (op: RichPipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoRichPipesFunctionToOperation = (op: (RichPipe, RichPipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipesToRichPipeFunctionToOperation = (op: (RichPipe, RichPipe) => Pipe) => new TwoPipesOperation( op(_, _) )

  implicit val fromThreeRichPipesFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipesToPipeFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromRichPipeListFunctionToOperation = (op: List[RichPipe] => RichPipe) => new ListRichPipesOperation({ seq : Seq[RichPipe] => op(seq.toList).pipe })
  implicit val fromRichPipeListToPipeFunctionToOperation = (op: List[RichPipe] => Pipe) => new ListRichPipesOperation({ seq : Seq[RichPipe] => op(seq.toList) })

  implicit val fromRichPipeSeqFunctionToOperation = (op: Seq[RichPipe] => RichPipe) => new ListRichPipesOperation(op(_).pipe)
  implicit val fromRichPipeSeqToPipeFunctionToOperation = (op: Seq[RichPipe] => Pipe) => new ListRichPipesOperation(op(_))


  import Dsl._
  implicit val fromSinglePipeFunctionToOperation = (op: Pipe => RichPipe) => new OnePipeOperation( pipe => op( RichPipe(pipe) ))
  implicit val fromSinglePipeToRichPipeFunctionToOperation = (op: Pipe => Pipe) => new OnePipeOperation( pipe=> RichPipe( op( RichPipe(pipe) ) ))

  implicit val fromTwoPipeFunctionToOperation = (op: (Pipe, Pipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipeToPipeFunctionToOperation = (op: (Pipe, Pipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreePipeFunctionToOperation = (op: (Pipe, Pipe, Pipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipeToPipeFunctionToOperation = (op: (Pipe, Pipe, Pipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromSeqPipeFunctionToOperation = (op: Seq[Pipe] => RichPipe) => new ListPipesOperation(op(_).pipe)
  implicit val fromSeqRichPipeToPipeFunctionToOperation = (op: Seq[Pipe] => Pipe) => new ListPipesOperation(op(_))

  implicit val fromListPipeFunctionToOperation = (op: List[Pipe] => RichPipe) => new ListPipesOperation( { seq : Seq[Pipe] => op(seq.toList) } )
  implicit val fromListRichPipeToPipeFunctionToOperation = (op: List[Pipe] => Pipe) => new ListPipesOperation( { seq : Seq[Pipe] => op(seq.toList) } )
}

