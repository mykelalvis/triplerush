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

import language.implicitConversions
import java.util.concurrent.atomic.AtomicInteger

object QueryIds {
  private val maxFullQueryId = new AtomicInteger(0)
  private val minFullQueryId = new AtomicInteger(0)
  def nextQueryId: Int = maxFullQueryId.incrementAndGet
  def nextCountQueryId: Int = minFullQueryId.decrementAndGet
}

object ParticleDebug {
  def apply(p: Array[Int]): ParticleDebug = {
    val particle = new QueryParticle(p)
    ParticleDebug(
      particle.queryId,
      particle.tickets,
      (-1 to -particle.bindings.length by -1).zip(particle.bindings).filter(_._2 != 0).toMap,
      particle.patterns.toList.reverse,
      particle.numberOfBindings,
      particle.r.length - 4)
  }

  def validate(p: Array[Int], msg: String) {
    import QueryParticle._
    val valid = (p.length - 4 - p.numberOfBindings) % 3 == 0
    if (!valid) {
      val debug = ParticleDebug(p)
      throw new Exception(s"$msg invalid particle: $debug")
    }
  }
}

case class ParticleDebug(
  id: Int,
  tickets: Long,
  bindings: Map[Int, Int],
  unmatched: List[TriplePattern],
  numberOfBindings: Int,
  intsForBindingsAndUnmatched: Int)

object QueryParticle {
  implicit def arrayToParticle(a: Array[Int]) = new QueryParticle(a)

  //    val queryId: Int = {
  //      if (withBindings) {
  //        QueryIds.nextQueryId
  //      } else {
  //        QueryIds.nextCountQueryId
  //      }
  //    }

  def fromSpecification(queryId: Int, s: QuerySpecification): Array[Int] = {
    if (queryId > 0) {
      val variableCount = math.abs(s.unmatched.foldLeft(0) {
        (currentMin, next) =>
          val minCandidate = math.min(next.o, math.min(next.s, next.p))
          math.min(currentMin, minCandidate)
      })
      QueryParticle(
        queryId,
        s.tickets,
        new Array[Int](variableCount),
        s.unmatched)
    } else {
      val ints = 4 + 3 * s.unmatched.length
      val r = new Array[Int](ints)
      r.writeQueryId(queryId)
      r.writeTickets(s.tickets)
      r.writePatterns(s.unmatched)
      r
    }

  }

  def apply(
    queryId: Int,
    tickets: Long = Long.MaxValue, // normal queries have a lot of tickets
    bindings: Array[Int],
    unmatched: Seq[TriplePattern]): Array[Int] = {
    val ints = 4 + bindings.length + 3 * unmatched.length
    val r = new Array[Int](ints)
    r.writeQueryId(queryId)
    r.writeTickets(tickets)
    r.writeBindings(bindings)
    r.writePatterns(unmatched)

    r
  }

  val failed = null.asInstanceOf[Array[Int]]

}

/**
 * The array packs:
 * 0 int:    queryId,
 * 1-2 long:   tickets (long encoded as 2 ints)
 * 3 int:    numberOfBindings
 * 4-? ints:   bindings
 * ? ?*3 ints: triple patterns in reverse matching order (last one
 *           gets matched first).
 */
class QueryParticle(val r: Array[Int]) extends AnyVal {

  import QueryParticle._

  def validate(msg: String) = ParticleDebug.validate(r, msg)

  def isBindingQuery = queryId > 0

  def copy: Array[Int] = {
    validate("pre-copy")
    val c = new Array[Int](r.length)
    System.arraycopy(r, 0, c, 0, r.length)
    ParticleDebug.validate(c, "post-copy")
    c
  }

  def bindSubject(
    toMatchS: Int,
    toMatchP: Int,
    toMatchO: Int,
    toBindS: Int,
    toBindP: Int,
    toBindO: Int,
    copyBeforeWrite: Boolean): Array[Int] = {
    validate("pre-bindSubject")
    // Subject is conflicting constant. No binding possible.
    if (toMatchS > 0) {
      return QueryParticle.failed
    }

    // Subject is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = toMatchS
    val boundValue = toBindS

    if (isBindingIncompatible(toMatchP, toBindP, variable, boundValue)
      || isBindingIncompatible(toMatchO, toBindO, variable, boundValue)) {
      return QueryParticle.failed
    }

    // No conflicts, we bind the value to the variable.
    val currentParticle: Array[Int] = {
      if (copyBeforeWrite) {
        copyWithoutLastPattern
      } else {
        r
      }
    }
    if (isBindingQuery) {
      val variableIndex = -(variable + 1)
      currentParticle.writeBinding(variableIndex, boundValue)
    }
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    val result = currentParticle.bindPredicate(toMatchP, toMatchO, toBindP, toBindO, false)
    ParticleDebug.validate(result, "post-bindSubject")
    result
  }

  def bindPredicate(
    toMatchP: Int,
    toMatchO: Int,
    toBindP: Int,
    toBindO: Int,
    copyBeforeWrite: Boolean): Array[Int] = {
    validate("pre-bindPredicate")
    if (toMatchP == toBindP) { // Predicate is compatible constant. No binding necessary. 
      return bindObject(toMatchO, toBindO, copyBeforeWrite)
    }

    // Predicate is conflicting constant. No binding possible.
    if (toMatchP > 0) {
      return QueryParticle.failed
    }

    // Predicate is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = toMatchP
    val boundValue = toBindP

    if (isBindingIncompatible(toMatchO, toBindO, variable, boundValue)) {
      return QueryParticle.failed
    }

    // No conflicts, we bind the value to the variable.
    val currentParticle = {
      if (copyBeforeWrite) {
        copyWithoutLastPattern
      } else {
        r
      }
    }
    if (isBindingQuery) {
      val variableIndex = -(variable + 1)
      currentParticle.writeBinding(variableIndex, boundValue)
    }
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    val result = currentParticle.bindObject(toMatchO, toBindO, false)
    ParticleDebug.validate(result, "post-bindPredicate")
    result
  }

  def bindObject(
    toMatchO: Int,
    toBindO: Int,
    copyBeforeWrite: Boolean): Array[Int] = {
    validate("pre-bindObject")
    if (toMatchO == toBindO) { // Object is compatible constant. No binding necessary. 
      //      val result = {
      //        if (copyBeforeWrite) {
      //          // We need to cut off the last pattern, even if we never write.
      //          particle.copyWithoutLastPattern
      //        } else {
      //          particle
      //        }
      //      }
      // In theory the check above would be necessary. In practice this
      // execution path is only reached if a particle copy was made before.
      return r
    }

    // Object is conflicting constant. No binding possible.
    if (toMatchO > 0) {
      return QueryParticle.failed
    }

    // Object is a variable that needs to be bound to the constant in the triple pattern. 
    // Bind value to variable.
    val variable = toMatchO
    val boundValue = toBindO

    // No conflicts, we bind the value to the variable.
    val currentParticle = {
      if (copyBeforeWrite) {
        copyWithoutLastPattern
      } else {
        r
      }
    }
    if (isBindingQuery) {
      val variableIndex = -(variable + 1)
      currentParticle.writeBinding(variableIndex, boundValue)
    }
    currentParticle.bindVariablesInPatterns(variable, boundValue)
    ParticleDebug.validate(currentParticle, "post-bindObject")
    currentParticle
  }

  // If the variable appears multiple times in the same pattern, then all the bindings have to be compatible.  
  @inline private def isBindingIncompatible(otherAttribute: Int, tpAttribute: Int, variable: Int, boundValue: Int) = (otherAttribute == variable && tpAttribute != boundValue)

  // Updates an attribute with a new binding.
  @inline private def updatedAttribute(attribute: Int, variable: Int, boundValue: Int) = if (attribute == variable) boundValue else attribute

  def bindings: Array[Int] = {
    val numBindings = numberOfBindings
    val b = new Array[Int](numBindings)
    System.arraycopy(r, 4, b, 0, numBindings)
    b
  }
  def isResult = r.length == 4 + numberOfBindings
  def queryId: Int = r(0)
  def writeQueryId(id: Int) = r(0) = id
  def tickets: Long = {
    ((r(1) | 0l) << 32) | (r(2) & 0x00000000FFFFFFFFL)
  }
  def writeTickets(t: Long) = {
    r(1) = (t >> 32).toInt
    r(2) = t.toInt
  }
  def numberOfBindings: Int = r(3)
  def writeBindings(bindings: Seq[Int]) {
    r(3) = bindings.length
    var i = 0
    while (i < bindings.length) {
      writeBinding(i, bindings(i))
      i += 1
    }
  }
  def writeBinding(bindingIndex: Int, boundValue: Int) {
    val baseBindingIndex = 4
    r(baseBindingIndex + bindingIndex) = boundValue
  }
  def binding(bindingIndex: Int): Int = {
    val contentIntIndex = bindingIndex + 4
    r(contentIntIndex)
  }

  def writePatterns(unmatched: Seq[TriplePattern]) {
    var i = 0
    var tpByteIndex = r.length - 3 // index of subject of last pattern
    while (i < unmatched.length) {
      writePattern(tpByteIndex, unmatched(i))
      tpByteIndex -= 3
      i += 1
    }
  }

  /**
   * Requires the index where the subject will be written.
   * Pattern is written in spo order.
   */
  def writePattern(subjectIndex: Int, p: TriplePattern) {
    r(subjectIndex) = p.s
    r(subjectIndex + 1) = p.p
    r(subjectIndex + 2) = p.o
  }

  def copyWithTickets(t: Long, complete: Boolean): Array[Int] = {
    validate("pre-copyWithTickets")
    val newR = r.clone
    if (complete) {
      newR.writeTickets(t)
    } else {
      newR.writeTickets(-t)
    }
    ParticleDebug.validate(newR, "post-copyWithTickets")
    newR
  }

  def patterns = {
    for (i <- numberOfPatterns - 1 to 0 by -1) yield pattern(i)
  }

  def numberOfPatterns: Int = (r.length - 4 - numberOfBindings) / 3

  def pattern(index: Int) = {
    val sIndex = 3 * index + 4 + numberOfBindings
    val pIndex = sIndex + 1
    val oIndex = sIndex + 2
    TriplePattern(r(sIndex), r(pIndex), r(oIndex))
  }

  def lastPattern: TriplePattern = {
    if (r.length < 7 + numberOfBindings) {
      throw new Exception(s"There is no last pattern to access:" +
        s"queryId=$queryId, tickets=$tickets numberOfBindings=$numberOfBindings length=${r.length}")
    }
    val sIndex = r.length - 3
    val pIndex = r.length - 2
    val oIndex = r.length - 1
    TriplePattern(
      r(sIndex),
      r(pIndex),
      r(oIndex))
  }

  def copyWithoutLastPattern: Array[Int] = {
    validate("pre-copyWithoutLastPattern")
    //assert(r.length >= 7 + numberOfBindings, "There is no TriplePattern to remove left.")
    if (r.length < 7 + numberOfBindings) {
      val debug = ParticleDebug(r)
      throw new Exception(s"There is no TriplePattern to remove left: $r")
    }
    val copyLength = r.length - 3
    val rCopy = new Array[Int](copyLength)
    System.arraycopy(r, 0, rCopy, 0, copyLength)
    validate("post-copyWithoutLastPattern")
    rCopy
  }

  // Update all patterns with this new binding.
  def bindVariablesInPatterns(
    variable: Int,
    boundValue: Int) {
    // Index of first subject of first TP.
    var i = numberOfBindings + 4
    while (i < r.length) {
      if (r(i) == variable) {
        r(i) = boundValue
      }
      i += 1
    }
  }

  /**
   *  Routing address for this query.
   */
  def routingAddress: Any = {
    if (isResult) {
      queryId
    } else {
      // Query not complete yet, route onwards.
      val s = lastPatternS
      val p = lastPatternP
      val o = lastPatternO
      if (s > 0 && p > 0 && o > 0) {
        TriplePattern(s, 0, o)
      } else {
        TriplePattern(math.max(s, 0), math.max(p, 0), math.max(o, 0))
      }
    }
  }

  /**
   * Checks that the last pattern is not fully bound and that no variable appears multiple times in the last pattern.
   * The same variable appearing multiple times might cause a binding to fail.
   */
  @inline def isSimpleToBind = {
    val s = lastPatternS
    val p = lastPatternP
    val o = lastPatternO
    !(s > 0 && p > 0 && o > 0) &&
      (s < 0 || (s != p && s != o)) &&
      (o < 0 || (o != p))
  }

  @inline def lastPatternS = r(r.length - 3)
  @inline def lastPatternP = r(r.length - 2)
  @inline def lastPatternO = r(r.length - 1)

  /**
   * Assumption: TP has all constants.
   */
  def bind(toBindS: Int, toBindP: Int, toBindO: Int): Array[Int] = {
    val patternS = lastPatternS
    val patternP = lastPatternP
    val patternO = lastPatternO
    if (toBindS == patternS) { // Subject is compatible constant. No binding necessary. 
      if (toBindP == patternP) { // Predicate is compatible constant. No binding necessary. 
        if (toBindO == patternO) { // Object is compatible constant. No binding necessary. 
          return copyWithoutLastPattern
        }
        return bindObject(patternO, toBindO, true)
      }
      return bindPredicate(patternP, patternO, toBindP, toBindO, true)
    }
    return bindSubject(patternS, patternP, patternO, toBindS, toBindP, toBindO, true)
  }
}
  
