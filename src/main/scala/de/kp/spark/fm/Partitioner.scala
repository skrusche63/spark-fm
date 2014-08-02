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

import org.apache.spark.{RangePartitioner,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

object Partitioner {

  /**
   * Read data from file and use K-Means algorithm to cluster
   * the raw dataset into a number of clusters, where each
   * cluster represents similiar data
   */  
  def buildKMeansPartitions(sc:SparkContext,input:String,clusters:Int,iterations:Int):RDD[(Int,(Double,SparseVector))] = {
    
    /**
     * Read dataset
     */
    val ds = sc.textFile(input).map(line => {
    
      val parts = line.split(',')
      
      val target   = parts(0).toDouble
      val features = parts(1).trim().split(' ').map(_.toDouble)
      
      (target,features)
    
    })
    
    /**
     * Convert dataset into vectors and train KMeans model
     */
    val vectors = ds.map(data => Vectors.dense(data._2))
    val model = KMeans.train(vectors, clusters, iterations)

    /**
     * Apply KMeans model
     */
    val clusteredDS = ds.map(data => {
      
      val (target,features) = data
      
      val cluster = model.predict(Vectors.dense(features))
      (cluster,(target,buildSparseVector(features)))
      
    })
    /**
     * Used to define how the elements in a key/value pair RDD are partitioned by key. 
     * Each key is mapped to a partition ID, which is an Int within 0 until numPartitions
     */   
    clusteredDS.partitionBy(new RangePartitioner(clusters,clusteredDS))
    
  }

  /**
   * Read data from file and use a simple mechanism to partition 
   * input data into a set of almost equal sized datasets
   */  
  def buildRandomPartitions(sc:SparkContext,input:String,num_partitions:Int):RDD[(Int,(Double,SparseVector))] = {
    
    val randomizedDS = sc.textFile(input).map(line => {
    
      val ix = new java.util.Random().nextInt(num_partitions)
      
      val parts = line.split(',')
      
      val target   = parts(0).toDouble
      val features = parts(1).trim().split(' ').map(_.toDouble)

      (ix, (target,buildSparseVector(features)))
   
    })
    
    randomizedDS.partitionBy(new RangePartitioner(num_partitions,randomizedDS))
    
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