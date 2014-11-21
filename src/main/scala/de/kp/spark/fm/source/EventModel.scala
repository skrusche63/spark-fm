package de.kp.spark.fm.source
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

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.fm.{Configuration,SparseVector}

class EventModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(uid:String,rawset:RDD[Map[String,String]],partitions:Int):RDD[(Int,(Double,SparseVector))] = {
    /* not implemented yet */
    null
  }
  
  def buildFile(uid:String,rawset:RDD[String],partitions:Int):RDD[(Int,(Double,SparseVector))] = {
    /* not implemented yet */
    null
  }
  
  def buildJDBC(uid:String,rawset:RDD[Map[String,Any]],partitions:Int):RDD[(Int,(Double,SparseVector))] = {
    /* not implemented yet */
    null
  }

  /**
   * This is a helper method to build a sparse vector from the input data;
   * to this end, we reduce to such entries that are different from zero
   */
  private def buildSparseVector(feature:Array[Double]):SparseVector = {
    
    val vector = new SparseVector(feature.length)
    
    for (i <- 0 until feature.length) {
    	
      val array_i: Double = feature(i)
      if (array_i > 0) vector.update(i, array_i)
        
    }
    
    vector
  
  }

}