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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.fm.{Configuration,SparseVector}
import de.kp.spark.fm.spec.{Fields}

import scala.collection.mutable.{ArrayBuffer,WrappedArray}

class FeatureModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]],partitions:Int):RDD[(Int,(Double,SparseVector))] = {
    
    val spec = sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).toLong
      val col = data(spec.value(Names.COL_FIELD)).toLong

      val label = data(spec.value(Names.LBL_FIELD)).toDouble
      val value = data(spec.value(Names.VAL_FIELD)).toDouble
      
      (row,col,label,value)
      
    })
    
    val targetedPoints = buildTargetedPoints(dataset)
    val num_partitions = sc.broadcast(partitions)
        
    val randomizedDS = targetedPoints.map{case(target,features) => {
      
      val ix = new java.util.Random().nextInt(num_partitions.value)
      (ix, (target,features))

    }}
    
    val partitioner = new Partitioner() {
      
      def numPartitions = num_partitions.value
      def getPartition(key: Any) = key.asInstanceOf[Int]
      
    }
    
    randomizedDS.partitionBy(partitioner)

  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String],partitions:Int):RDD[(Int,(Double,SparseVector))] = {

    val num_partitions = sc.broadcast(partitions)
    val randomizedDS = rawset.map(line => {
    
      val ix = new java.util.Random().nextInt(num_partitions.value)
      
      val parts = line.split(',')
      
      val target   = parts(0).toDouble
      val features = parts(1).trim().split(' ').map(_.toDouble)

      (ix, (target,buildSparseVector(features)))
   
    })
    
    val partitioner = new Partitioner() {
      
      def numPartitions = num_partitions.value
      def getPartition(key: Any) = key.asInstanceOf[Int]
      
    }
    
    randomizedDS.partitionBy(partitioner)
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]],partitions:Int):RDD[(Int,(Double,SparseVector))] = {

    val spec = sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
      val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

      val label = data(spec.value(Names.LBL_FIELD)).asInstanceOf[Double]
      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[Double]
      
      (row,col,label,value)
      
    })
    
    val targetedPoints = buildTargetedPoints(dataset)
    val num_partitions = sc.broadcast(partitions)
        
    val randomizedDS = targetedPoints.map{case(target,features) => {
      
      val ix = new java.util.Random().nextInt(num_partitions.value)
      (ix, (target,features))

    }}
    
    val partitioner = new Partitioner() {
      
      def numPartitions = num_partitions.value
      def getPartition(key: Any) = key.asInstanceOf[Int]
      
    }
    
    randomizedDS.partitionBy(partitioner)
    
  }
  
  def buildParquet(req:ServiceRequest,rawset:RDD[Map[String,Any]],partitions:Int):RDD[(Int,(Double,SparseVector))] = {

    val spec = sc.broadcast(Fields.get(req))
    /*
     * The parquet file can be described in two different ways:
     * 
     * (a) the feature part is specified as an array of values
     * 
     * (b) the feature part is specified by individual fields;
     * in this case the field value must be Doubles
     * 
     */
    val targetedPoints = if (rawset.take(1).head.size == 2) {
     /* 'data' specifies a map of two entries, where the first represents the 
      * target variable and the second one the predictor or feature variables
      * as an array; this parquet file format is used, if it specifies by a 
      * targeted point
      */
     rawset.map(data => {
       
       val seq = data.toSeq
        
        val target = seq(0)._2.asInstanceOf[Double]
        val features = seq(1)._2.asInstanceOf[WrappedArray[Double]]
 
        (target,buildSparseVector(features.toArray))
     
     })
    
    } else {
    
      val dataset = rawset.map(data => {
      
        val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
        val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

        val label = data(spec.value(Names.LBL_FIELD)).asInstanceOf[Double]
        val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[Double]
      
        (row,col,label,value)
      
      })
    
      buildTargetedPoints(dataset)
      
    }
    
    val num_partitions = sc.broadcast(partitions)        
    val randomizedDS = targetedPoints.map{case(target,features) => {
      
      val ix = new java.util.Random().nextInt(num_partitions.value)
      (ix, (target,features))

    }}
    
    val partitioner = new Partitioner() {
      
      def numPartitions = num_partitions.value
      def getPartition(key: Any) = key.asInstanceOf[Int]
      
    }
    
    randomizedDS.partitionBy(partitioner)
    
  }

  private def buildTargetedPoints(dataset:RDD[(Long,Long,Double,Double)]):RDD[(Double,SparseVector)] = {
  
    /*
     * The dataset specifies a 'sparse' data description;
     * in order to generate dense vectors from it, we first
     * have to determine the minimum (= 0) and maximum column
     * value to create equal size vectors
     */
    val size = sc.broadcast((dataset.map(_._2).max + 1).toInt)
    
    dataset.groupBy(x => x._1).map(x => {
      
      val row = x._1
      /*
       * The label is a denormalized value and is assigned to
       * each column specific dataset as well; this implies
       * that we only need this value once
       */
      val label = x._2.head._3
      val features = Array.fill[Double](size.value)(0)
      
      val data = x._2.map(v => (v._2.toInt,v._4)).toSeq.sortBy(v => v._1)
      data.foreach(x => features(x._1) = x._2)
      
      (label,buildSparseVector(features))
      
    })
 
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