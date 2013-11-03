package com.pragmasoft.scaldingunit

import com.twitter.scalding.{RichPipe, FieldConversions, TupleConversions}
import cascading.pipe.Pipe
import scala.language.implicitConversions

trait PipeOperations extends TupleConversions with FieldConversions with Serializable {
  implicit def p2rp(pipe: Pipe) = new RichPipe(pipe)

  implicit def rp2p(rich: RichPipe) = rich.pipe
}
