/*
 *  @author Philip Stutz
 *  
 *  Copyright 2015 iHealth Technologies
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

package com.signalcollect.triplerush.sparql

import scala.collection.JavaConversions.asJavaIterator
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.hp.hpl.jena.graph.{ GraphEvents, GraphStatisticsHandler, Node, Node_ANY, Node_Literal, Node_URI, Triple }
import com.hp.hpl.jena.graph.impl.GraphBase
import com.hp.hpl.jena.query.ARQ
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.sparql.engine.main.StageGenerator
import com.hp.hpl.jena.util.iterator.{ ExtendedIterator, WrappedIterator }
import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.hp.hpl.jena.graph.Node_Blank
import com.hp.hpl.jena.graph.Node_Variable

/**
 * A TripleRush implementation of the Jena Graph interface.
 */
class TripleRushGraph(val tr: TripleRush = new TripleRush) extends GraphBase with GraphStatisticsHandler {

  def getModel = ModelFactory.createModelForGraph(this)

  // Set TripleRushStageGenerator as default for all queries.
  val tripleRushStageGen = ARQ.getContext.get(ARQ.stageGenerator) match {
    case g: TripleRushStageGenerator =>
      new TripleRushStageGenerator(g.other)
    case otherGraph: StageGenerator =>
      new TripleRushStageGenerator(otherGraph)
    case _: Any => throw new Exception("No valid stage generator found.")
  }
  ARQ.getContext.set(ARQ.stageGenerator, tripleRushStageGen)

  def getStatistic(s: Node, p: Node, o: Node): Long = {
    val q = Seq(arqNodesToPattern(s, p, o))
    val countOptionFuture = tr.executeCountingQuery(q)
    val countOption = Await.result(countOptionFuture, 300.seconds)
    val count = countOption.getOrElse(throw new Exception(s"Incomplete counting query execution for $q."))
    count
  }

  override def createStatisticsHandler = this

  /**
   * Meaning of prefixes in the encoded string:
   * - Everything that starts with a letter is interpreted as an IRI,
   * because their schema has to start with a letter.
   * - If a string starts with a digit or a hyphen, then it is interpreted as an integer literal.
   * - If a string starts with `"` or "<", then it is interpreted as a general literal.
   */
  override def performAdd(triple: Triple): Unit = {
    val sString = NodeConversion.nodeToString(triple.getSubject)
    val pString = NodeConversion.nodeToString(triple.getPredicate)
    val oString = NodeConversion.nodeToString(triple.getObject)
    tr.addTriple(sString, pString, oString)
  }

  override def clear: Unit = {
    getEventManager.notifyEvent(this, GraphEvents.removeAll)
    tr.clear
  }

  override def close: Unit = {
    super.close
    tr.shutdown
  }

  def graphBaseFind(triplePattern: Triple): ExtendedIterator[Triple] = {
    println(s"graphBaseFind($triplePattern)")
    val s = triplePattern.getSubject
    val p = triplePattern.getPredicate
    val o = triplePattern.getObject
    val pattern = arqNodesToPattern(s, p, o)
    val resultIterator = tr.resultIteratorForQuery(Seq(pattern))
    val concreteS = if (s.isConcrete) Some(NodeConversion.nodeToString(s)) else None
    val concreteP = if (p.isConcrete) Some(NodeConversion.nodeToString(p)) else None
    val concreteO = if (o.isConcrete) Some(NodeConversion.nodeToString(o)) else None
    val convertedIterator = TripleRushIterator.convert(concreteS, concreteP, concreteO, tr.dictionary, resultIterator)
    WrappedIterator.createNoRemove(convertedIterator)
  }

  override def graphBaseContains(t: Triple): Boolean = {
    getStatistic(t.getSubject, t.getPredicate, t.getObject) >= 1
  }

  override def graphBaseSize: Int = {
    val wildcard = Node.ANY
    val sizeAsLong = getStatistic(wildcard, wildcard, wildcard)
    if (sizeAsLong <= Int.MaxValue) {
      sizeAsLong.toInt
    } else {
      Int.MaxValue // Better than crashing?
    }
  }

  // TODO: Make more efficient by unrolling everything.
  private def arqNodesToPattern(s: Node, p: Node, o: Node): TriplePattern = {
    var nextVariableId = -1
    @inline def nodeToId(n: Node): Int = {
      n match {
        case variable: Node_ANY =>
          val id = nextVariableId
          nextVariableId -= 1
          id
        case variable: Node_Variable =>
          throw new UnsupportedOperationException("Variable nodes are not supported in find patterns.")
        case blank: Node_Blank =>
          throw new UnsupportedOperationException("Blank nodes are not supported in find patterns.")
        case other =>
          tr.dictionary(NodeConversion.nodeToString(other))
      }
    }
    val sId = nodeToId(s)
    val pId = nodeToId(p)
    val oId = nodeToId(o)
    TriplePattern(sId, pId, oId)
  }

}
