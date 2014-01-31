package com.pragmasoft.scaldingunit.testmode

import com.twitter.scalding._
import scala.collection.mutable.Buffer
import scala.util.{Success, Failure, Try}
import scala.util.Success
import scala.util.Failure
import scala.Some
import scala.util.Success
import scala.util.Failure
import scala.Some

class EmptyJob(args: Args) extends Job(args)

class JobTestSpy extends JobTest(args => new EmptyJob(args)) with Serializable {
  protected var delegate: Option[JobTest] = None

  def setDelegate(delegate: JobTest) = this.delegate = Some(delegate)

  var runInHadoopMode : Option[Boolean] = None

  override def source(s : Source, iTuple : Iterable[Product])  = delegate.get.source(s, iTuple)
  override def source[T](s : Source, iTuple : Iterable[T])(implicit setter: TupleSetter[T]) = delegate.get.source(s, iTuple)(setter)

  override def sink[A](s : Source)(op : Buffer[A] => Unit )(implicit conv : TupleConverter[A]) = delegate.get.sink(s)(op)(conv)

  override def run = {
    runInHadoopMode = Some(false)
    delegate.get.run
  }

  override def runHadoop = {
    runInHadoopMode = Some(true)
    delegate.get.runHadoop
  }

  def hasBeenRunInHadoopMode : Try[Boolean] = runInHadoopMode match {
    case None => Failure(new IllegalStateException("Job hasn't been run"))
    case Some(result) => Success(result)
  }
}

object JobTestSpy {
  def apply() : JobTestSpy = new JobTestSpy
}