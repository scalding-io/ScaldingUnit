package com.pragmasoft.scaldingunit.testmode

trait WithTestMode {
  def testMode : TestRunMode
  def __jobTestSpy : Option[JobTestSpy]
}
