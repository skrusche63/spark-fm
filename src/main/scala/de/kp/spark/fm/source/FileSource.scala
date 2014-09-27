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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.fm.{Configuration,Partitioner,SparseVector}

class FileSource(@transient sc:SparkContext) extends Source(sc) {

  val input = Configuration.file()
  
  /**
   * Read data from file system: it is expected that the lines with
   * the respective text file are already formatted in the SPMF form
   */
  override def connect(params:Map[String,Any] = Map.empty[String,Any]):RDD[(Int,(Double,SparseVector))] = {
    
    val num_partitions = params("num_partitions").asInstanceOf[Int]
    Partitioner.buildRandomPartitions(sc,input,num_partitions)
   
  }
  
}