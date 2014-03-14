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

package com.signalcollect.triplerush.optimizers

import com.signalcollect.triplerush.TriplePattern
import scala.Array.canBuildFrom

object GreedyCardinalityOptimizer extends Optimizer {

  //bpo::
  def optimize(cardinalities: Map[TriplePattern, Long], edgeCounts: Map[Int, Long], maxObjectCounts: Map[Int, Long], maxSubjectCounts: Map[Int, Long]): Array[TriplePattern] = {
    // Sort triple patterns by cardinalities and send the query to the most selective pattern first.
    var sortedPatterns = cardinalities.toArray.sortBy(_._2)
    sortedPatterns.map(_._1)
  }

}
