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

import de.kp.spark.fm.hadoop.HadoopIO

class FMModel {

  private var c:Double = 0

  private var v:DenseVector = null
  private var m:DenseMatrix = null
  
  private var p:Map[String,Any] = null
  
  def this(c:Double,v:DenseVector,m:DenseMatrix,p:Map[String,Any]) {
    this()
    
    this.c = c
    
    this.v = v
    this.m = m
    
    this.p = p
    
  }
  def save(path:String) {
    HadoopIO.writeFM(c, v, m, p, path)    
  }
  
  /**
   * Load factorization model from Hadoop sequence file;
   * to this end provide path to file on HDFS, note, that
   * this path is managed by the Redis instance
   */
  def load(path:String) {
    
    val (c,v,m,p) = HadoopIO.readFM(path)
    
    this.c = c
    
    this.v = v
    this.m = m
    
    this.p = p
    
  }

  def factors = (c,v,m,p)
  
  def predict(data:Array[Double]): Double = {
    
    val k0 = p("k0").asInstanceOf[Boolean]
    val k1 = p("k1").asInstanceOf[Boolean]
     
    val num_factor = p("num_factor").asInstanceOf[Int]
    FM.predict(data,c,v,m,num_factor,k0,k1)
    
  }

}