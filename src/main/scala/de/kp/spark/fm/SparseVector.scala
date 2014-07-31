package de.kp.spark.fm
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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * A SparseVector is an in-memory optimization for arrays, which contain 
 * only few values other than zero. 
 * 
 * HashMap based implementations are not recommended as performance is 
 * 3 to 4 times slower than an original array
 */
class SparseVector(size:Int) extends Serializable {
	
  /**
   * Linked arrays for indexes and values
   */
  private val indexes = ArrayBuffer.empty[Int]
  private val values = ArrayBuffer.empty[Double]
    
  /**
   * The overall length of the (original) array
   */
  private val length:Int = size
  
  /**
   * Get value from array position 'index'
   */
  def apply(index: Int): Double = {
    
    var res: Double = 0.0
    
    breakable {
	    for (i <-0 until indexes.size) {
	      if (indexes(i) == index) {	        
	        res = values(i)
	        break()	        
	      }
	    }	    
    }
    
    res
    
  }
  
  def updateIndex(index:Int) = indexes += index

  def updateValue(value:Double) = values += value

  def update(index: Int, value:Double) = {
    indexes += index
    values  += value
  }
  
  /**
   * Dimensions
   */
  def size() = length
  
  def indexDim = indexes.size  
  def valueDim = values.size
  
  def getIndexes():ArrayBuffer[Int] = indexes
   
  def getValues():ArrayBuffer[Double] = values
 
  override def toString: String = {
    return ""
  }
}