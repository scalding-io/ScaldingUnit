package com.pragmasoft.scaldingunit

import com.twitter.scalding.{RichPipe, FieldConversions, TupleConversions}
import cascading.pipe.Pipe
import scala.language.implicitConversions

/**
 * Created with IntelliJ IDEA.
 * User: gfe01
 * Date: 21/10/13
 * Time: 11:14
 * To change this template use File | Settings | File Templates.
 */
trait PipeOperations extends TupleConversions with FieldConversions with Serializable {
  implicit def p2rp(pipe: Pipe) = new RichPipe(pipe)

  implicit def rp2p(rich: RichPipe) = rich.pipe
}
