package de.kp.spark.fm.prepare
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-FM project
* (https://github.com/skrusche63/spark-fm).
* 
* Spark-FM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-FM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-FM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import scala.collection.mutable.{Buffer,Map}

/**
 * TODO: This class is under construction 
 */
class Dict extends Serializable {

  /**
   * Reference to the external data designators is used
   * to speed up data access
   */
  private val terms  = Buffer.empty[String]
  private val lookup = Map.empty[String,Int]
  
  /**
   * Build a dictionary from a distint sequence of terms 
   */
  def build(seq:Seq[String]) {
  
    seq.map(entry => terms += entry)
    seq.zipWithIndex.map(entry => lookup += entry._1 -> entry._2)
    
  }
  /**
   * Retrieve lookup data structure
   */
  def getLookup = lookup
  /**
   * Retrieve external data structure
   */
  def getTerms = terms
  
  def size = terms.size
  
}