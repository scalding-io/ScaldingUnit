import sbt._
import Keys._

object Common {
  val deps = Seq(
		   "com.twitter"        %%  "scalding-core"       % "0.8.11"
		  ,"com.twitter"        %%  "scalding-commons"    % "0.2.0"
		  ,"org.scalatest"      %   "scalatest_2.10"      % "1.9.2" % "test"
		)
}