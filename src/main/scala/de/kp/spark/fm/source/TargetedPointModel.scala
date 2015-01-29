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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.fm._
import de.kp.spark.fm.spec.{Fields}

import scala.collection.mutable.{ArrayBuffer,WrappedArray}

class TargetedPointModel(@transient ctx:RequestContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {
    
    val spec = ctx.sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).toLong
      val col = data(spec.value(Names.COL_FIELD)).toLong

      val value = data(spec.value(Names.VAL_FIELD)).toDouble
      val category = data(spec.value(Names.CAT_FIELD)).toString
      
      (row,col,category,value)
      
    })
    
    format(dataset)

  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {

    val points = rawset.map(line => {
      
      val parts = line.split(',')
      
      val target   = parts(0).toDouble
      val features = parts(1).trim().split(' ').map(_.toDouble)

      val vector = new FMVector(features.length) 
      for (i <- 0 until features.length) {
    	
         val array_i: Double = features(i)
         if (array_i > 0) vector.update(i, array_i)
        
      }

      (target,vector)
   
    })
    
    (Seq.empty[FMBlock],points)
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {

    val spec = ctx.sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
      val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[Double]
      val category = data(spec.value(Names.CAT_FIELD)).asInstanceOf[String]
      
      (row,col,category,value)
      
    })
    
    format(dataset)
    
  }
  
  def buildParquet(req:ServiceRequest,rawset:RDD[Map[String,Any]]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {

    val spec = ctx.sc.broadcast(Fields.get(req))
    /*
     * The parquet file can be described in two different ways:
     * 
     * (a) the feature part is specified as an array of values
     * 
     * (b) the feature part is specified by individual fields;
     * in this case the field value must be Doubles
     * 
     */
    val (blocks,points) = if (rawset.take(1).head.size == 2) {
     /* 'data' specifies a map of two entries, where the first represents the 
      * target variable and the second one the predictor or feature variables
      * as an array; this parquet file format is used, if it specifies by a 
      * targeted point
      */
     val dataset = rawset.map(data => {
       
       val seq = data.toSeq
        
       val target = seq(0)._2.asInstanceOf[Double]
       val features = seq(1)._2.asInstanceOf[WrappedArray[Double]]
 
       val vector = new FMVector(features.length) 
       for (i <- 0 until features.length) {
    	
         val array_i: Double = features(i)
         if (array_i > 0) vector.update(i, array_i)
        
       }
       
       (target,vector)
     
     })
    
     (Seq.empty[FMBlock],dataset)
     
    } else {
    
      val dataset = rawset.map(data => {
      
        val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
        val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

        val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[Double]
        val category = data(spec.value(Names.CAT_FIELD)).asInstanceOf[String]
      
        (row,col,category,value)
      
      })
    
      format(dataset)
      
    }
    
    (blocks,points)
    
  }

  private def format(dataset:RDD[(Long,Long,String,Double)]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {
  
    /*
     * The dataset specifies a 'sparse' data description; in order to generate 
     * dense vectors from it, we first have to determine the minimum (= 0) and 
     * maximum column value to create equal size vectors.
     * 
     * The 'target' variable is the one with the highest column number, e.g.
     * col = 0,1,2,3,4, 5 = target. This indicates that the highest column
     * number also describes the size of the feature vector.
     * 
     * The following categories are supported:
     * 
     * - user
     * - item
     * - context
     * - label
     * 
     */
    val size = ctx.sc.broadcast((dataset.map(_._2).max).toInt)
    val rows = dataset.groupBy(x => x._1)
    
    /* 
     * STEP #1: Build metadata description from first row 
     */
    val metadata = rows.take(1).head._2.map{case(row,col,cat,value) => (cat,col)}
    val blocks = metadata.groupBy(_._1).map(x => {
      
      val cat = x._1
      val col = x._2.map(_._2).toSeq.sorted
      
      val begin = col.head
      val end   = col.last
      
      FMBlock(cat,begin,end)
      
    }).toSeq
     
    /* 
     * STEP #2: Build targeted points froms rows 
     */
    val points = rows.map(x => {
      
      val row = x._1
      val data = x._2.map(v => (v._2,v._4)).toSeq.sortBy(v => v._1)
      
      val values = data.init
      val target = data.last

      val points = new FMVector(size.value)      
      values.foreach{case (col,value) => {
        if (value > 0) points.update(col.toInt,value)
      }}

      (target._2,points)
      
    })
    
    (blocks,points)
    
  }

}