/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
    val query: PatternQuery,
    val promise: Promise[(List[PatternQuery], Map[String, Any])],
    val optimize: Boolean = true) extends ProcessingVertex[Int, PatternQuery](query.queryId) {

  val expectedTickets: Long = query.tickets
  val expectedCardinalities = query.unmatched.size

  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    if (optimize && query.unmatched.size > 1) {
      // Gather pattern cardinalities first.
      query.unmatched foreach (triplePattern => {
        val indexId = triplePattern.routingAddress
        graphEditor.sendSignal(CardinalityRequest(triplePattern, id), indexId, None)
      })
    } else {
      // Dispatch the query directly.
      graphEditor.sendSignal(query, query.unmatched.head.routingAddress, None)
    }
  }

  var cardinalities: Map[TriplePattern, Int] = Map()

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case query: PatternQuery =>
        processQuery(query)
      case CardinalityReply(forPattern, cardinality) =>
        cardinalities += forPattern -> cardinality
        if (cardinalities.size == expectedCardinalities) {
          // Sort triple patterns by cardinalities and send the query to the most selective pattern first.
          val sortedPatterns = cardinalities.toList sortBy (_._2) map (_._1)
          val reorderedQuery = query.withUnmatchedPatterns(sortedPatterns)
          graphEditor.sendSignal(reorderedQuery, sortedPatterns.head.routingAddress, None)
        }
    }
    true
  }

  //  var numberOfFailedQueries = 0
  //  var numberOfSuccessfulQueries = 0

  def processQuery(query: PatternQuery) {
    receivedTickets += query.tickets
    complete &&= query.isComplete
    if (query.unmatched.isEmpty) {
      // numberOfSuccessfulQueries += 1
      // println(s"Success: $query")
      // Query was matched successfully.
      if (firstResultNanoTime == 0) {
        firstResultNanoTime = System.nanoTime
      }
      state = query :: state
    } else {
      // numberOfFailedQueries += 1
      // println(s"Failure: $query")
    }
  }

  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    promise success (state, Map("firstResultNanoTime" -> firstResultNanoTime))
    //    val totalQueries = (numberOfFailedQueries + numberOfSuccessfulQueries).toDouble
    //    println(s"Total # of queries = $totalQueries failed : ${((numberOfFailedQueries / totalQueries) * 100.0).round}%")
    graphEditor.removeVertex(id)
  }

  def process(item: PatternQuery, graphEditor: GraphEditor[Any, Any]) {}

}