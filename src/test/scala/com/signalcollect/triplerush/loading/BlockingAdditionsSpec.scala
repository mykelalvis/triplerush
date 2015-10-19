package com.signalcollect.triplerush.loading

import java.io.File

import org.scalatest.{ Finders, FlatSpec }
import org.scalatest.concurrent.ScalaFutures

import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.signalcollect.triplerush.dictionary.HashDictionary
import com.signalcollect.util.TestAnnouncements

class BlockingAdditionsSpec extends FlatSpec with TestAnnouncements with ScalaFutures {

  "Blocking additions" should "correctly load triples from a file" in {
    //fastStart = true,
    val tr = TripleRush(dictionary = new HashDictionary())
    //val tr = TripleRush(dictionary = new ModularDictionary())
    try {
      val filePath = s".${File.separator}lubm${File.separator}university0_0.nt"
      println(s"Loading file $filePath ...")
      //tr.loadFromFile(filePath)
      val howMany = 25700
      //.take(howMany)
      tr.prepareExecution
      tr.addTriples(TripleIterator(filePath), blocking = true)
      tr.awaitIdle
      println(tr.dictionary)
      //tr.awaitIdle()
      //val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
      //          assert(count == 25700)
      //      val countOptionFuture = tr.executeCountingQuery(Seq(TriplePattern(-1, -2, -3)))
      //      whenReady(countOptionFuture) { countOption =>
      //        assert(countOption == Some(howMany))
      //      }
      println(tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size)
      println(tr.countVerticesByType)
      println(tr.edgesPerIndexType)
    } finally {
      tr.shutdown
    }
  }

}
