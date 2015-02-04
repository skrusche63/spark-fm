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

import org.apache.spark.rdd.RDD

object FMFormatter extends Serializable {

  def format(dataset:RDD[(Long,Long,String,Double)]):(Seq[FMBlock],RDD[(Double,FMVector)]) = {
  
    val sc = dataset.sparkContext
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
     *   
     *   this category describes the user part of feature vector
     *   and requires a binary (0,1) specification of the user
     *   
     * - item
     * 
     *   this category describes the item part of the feature vector
     *   and requires a binary (0,1) specification of the item
     * 
     * - context_numerical
     * 
     *   this category describes a single column of the context vector
     *   specifying a certain numerical value 
     * 
     * - context_categorical
     * 
     *   this category describes  a part of the context vector and
     *   requires a binary (0,1) specification of the context
     *   
     * - context_categorical_set
     * 
     *   this category describes a part of the context vector that
     *   describes sets of groups with weight factors that sum up to 1
     * 
     * - label
     * 
     */
    val size = sc.broadcast((dataset.map(_._2).max).toInt)
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