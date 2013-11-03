# ScaldingUnit

TDD utils for Scalding developers


The aim of this project is to allow user to write Scalding (https://github.com/twitter/scalding) map-reduce jobs in a more modular and test-driven way.
It is based on the experience done in the Big Data unity at BSkyB where it originated and is currently used and maintained.

A Scalding job consists in a series of transformations applied to one or more sources in order to create one or more
output resources or sinks. A very simple example taken from the Scalding documentations is as follows.

```scala
package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
    TextLine( args("input") )
     .flatMap('line -> 'word) { line : String => tokenize(line) }
     .groupBy('word) { _.size }
     .write( Tsv( args("output") ) )

    // Split a piece of text into individual words.
    def tokenize(text : String) : Array[String] = {
     // Lowercase each word and remove punctuation.
     text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
    }
}
```

The transformations are defined as operations on a cascading.pipe.Pipe class or on the richer wrapper com.twitter.scalding.RichPipe.
Scalding provides a way of testing Jobs via the com.twitter.scalding.JobTest class. This class allows to specify values for the different
Job sources and to specify assertions on the different job sinks.
This approach works very well to do end to end test on the Job and is good enough for small jobs as the one described above.
When the Job logic become more complex it is very helpful to decompose its work in simpler functions to be tested indipendently before being
aggregated into the Job.

A job will therefore