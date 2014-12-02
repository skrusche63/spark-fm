package de.kp.spark.fm.sink
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

import java.util.Date

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisClient

import de.kp.spark.fm.Configuration

import scala.collection.JavaConversions._

class RedisSink {

  val (host,port) = Configuration.redis
  val client = RedisClient(host,port.toInt)
  
  /**
   * Register offset and path to similarity matrix; the similarity
   * matrix is built from the interaction vectors of a feature set
   */
  def addMatrix(req:ServiceRequest,offset:String,matrix:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "matrix:" + req.data(Names.REQ_UID)
    val v = "" + timestamp + ":" + offset + ":" + matrix
    
    client.zadd(k,timestamp,v)
    
  }
  
  def matrixExists(req:ServiceRequest):Boolean = {

    val k = "matrix:" + req.data(Names.REQ_UID)
    client.exists(k)
    
  }
  
  def matrix(req:ServiceRequest):(String,String) = {

    val k = "matrix:" + req.data(Names.REQ_UID)
    val matrices = client.zrange(k, 0, -1)

    if (matrices.size() == 0) {
      null
    
    } else {
      
      val last = matrices.toList.last
      val Array(timestamp,offset,matrix) = last.split(":")
      
      (offset,matrix)
      
    }
  
  }
  
  /**
   * The trained factorization (or polynom) model is persisted to the 
   * HDFS file system; the REDIS instance holds the path to the model.
   * 
   * We do not use the service parameter to specify the respective path
   * as polynom model are exclusively built by the Context-Aware Engine
   */
  def addPolynom(req:ServiceRequest,model:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "polynom:" + req.data(Names.REQ_UID)
    val v = "" + timestamp + ":" + model
    
    client.zadd(k,timestamp,v)
    
  }
  
  def polynomExists(req:ServiceRequest):Boolean = {

    val k = "polynom:" + req.data(Names.REQ_UID)
    client.exists(k)
    
  }
  
  def polynom(req:ServiceRequest):String = {

    val k = "polynom:" + req.data(Names.REQ_UID)
    val polynoms = client.zrange(k, 0, -1)

    if (polynoms.size() == 0) {
      null
    
    } else {
      
      val last = polynoms.toList.last
      last.split(":")(1)
      
    }
  
  }

}