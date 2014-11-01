package de.kp.spark.fm.redis
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

import de.kp.spark.fm.model._
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  val service = "fm"

  def addFields(req:ServiceRequest,fields:Fields) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "fields:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeFields(fields)
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addStatus(uid:String, task:String, status:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addPolynom(uid:String,model:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "polynom:" + service + ":" + uid
    val v = "" + timestamp + ":" + model
    
    client.zadd(k,timestamp,v)
    
  }

  def fieldsExist(uid:String):Boolean = {

    val k = "fields:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def polynomExists(uid:String):Boolean = {

    val k = "polynom:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
 
  def fields(uid:String):Fields = {

    val k = "fields:" + service + ":" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      new Fields(List.empty[Field])
    
    } else {
      
      val fields = metas.toList.last
      Serializer.deserializeFields(fields)
      
    }

  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      0
    
    } else {
      
      val first = jobs.iterator().next()
      first.split(":")(0).toLong
      
    }
     
  }
  
  def polynom(uid:String):String = {

    val k = "polynom:" + service + ":" + uid
    val polynoms = client.zrange(k, 0, -1)

    if (polynoms.size() == 0) {
      null
    
    } else {
      
      val last = polynoms.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      null
    
    } else {
      
      val job = Serializer.deserializeJob(jobs.toList.last)
      job.status
      
    }

  }

}