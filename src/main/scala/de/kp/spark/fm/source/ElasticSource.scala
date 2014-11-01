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

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import de.kp.spark.fm.{Configuration,SparseVector}

import de.kp.spark.fm.io.ElasticReader

import de.kp.spark.fm.model._
import de.kp.spark.fm.spec.{Fields}

import scala.collection.mutable.ArrayBuffer

class ElasticSource(@transient sc:SparkContext) extends Source(sc) {
          
  /* Retrieve data from Elasticsearch */    
  val conf = Configuration.elastic                          
 
  override def connect(params:Map[String,String] = Map.empty[String,String]):RDD[(Int,(Double,SparseVector))] = {
    
    val uid = params("uid")    
    
    val index = params("source.index")
    val mapping = params("source.type")
    
    val query = params("query")
    
    val spec = sc.broadcast(Fields.get(uid))
    val num_partitions = sc.broadcast(params("num_partitions").toInt)
    
    /* Connect to Elasticsearch */
    val rawset = new ElasticReader(sc,index,mapping,query).read
    val randomizedDS = rawset.map(data => {
      
      val fields = spec.value
      val ix = new java.util.Random().nextInt(num_partitions.value)
 
      val target = data(fields.head).toDouble
      val features = ArrayBuffer.empty[Double]
      
      for (field <- fields.tail) {
        features += data(field).toDouble
      }

      (ix, (target,buildSparseVector(features.toArray)))
      
    })
    
    val partitioner = new Partitioner() {
      
      def numPartitions = num_partitions.value
      def getPartition(key: Any) = key.asInstanceOf[Int]
      
    }
    
    randomizedDS.partitionBy(partitioner)
    
  }

}