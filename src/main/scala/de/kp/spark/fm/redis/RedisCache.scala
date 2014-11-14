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

  def addFields(req:ServiceRequest,fields:Fields) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "fields:context:" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeFields(fields)
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addRequest(req:ServiceRequest) {
    
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "request:context"
    val v = "" + timestamp + ":" + Serializer.serializeRequest(req)
    
    client.lpush(k,v)
    
  }
  
  def addStatus(req:ServiceRequest, status:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:context:" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc("context",req.task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addPolynom(req:ServiceRequest,model:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "polynom:context:" + req.data("uid")
    val v = "" + timestamp + ":" + model
    
    client.zadd(k,timestamp,v)
    
  }

  def fieldsExist(uid:String):Boolean = {

    val k = "fields:context:" + uid
    client.exists(k)
    
  }
  
  def polynomExists(uid:String):Boolean = {

    val k = "polynom:context:" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:context:" + uid
    client.exists(k)
    
  }
 
  def fields(uid:String):Fields = {

    val k = "fields:context:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      new Fields(List.empty[Field])
    
    } else {
      
      val latest = metas.toList.last
      val Array(timestamp,fields) = latest.split(":")
      
      Serializer.deserializeFields(fields)
      
    }

  }
  
  def polynom(uid:String):String = {

    val k = "polynom:context:" + uid
    val polynoms = client.zrange(k, 0, -1)

    if (polynoms.size() == 0) {
      null
    
    } else {
      
      val last = polynoms.toList.last
      last.split(":")(1)
      
    }
  
  }
  
  def requestsTotal():Long = {

    val k = "request:context"
    if (client.exists(k)) client.llen(k) else 0
    
  }

  def requests(start:Long,end:Long):List[(Long,ServiceRequest)] = {
    
    val k = "request:context"
    val requests = client.lrange(k, start, end)
    
    requests.map(request => {
      
      val Array(ts,req) = request.split(":")
      (ts.toLong,Serializer.deserializeRequest(req))
      
    }).toList
    
  }
  
  def status(uid:String):String = {

    val k = "job:context:" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      /* Format: timestamp:jobdesc */
      val last = data.toList.last
      val Array(timestamp,jobdesc) = last.split(":")
      
      val job = Serializer.deserializeJob(jobdesc)
      job.status
      
    }

  }
  
  def statuses(uid:String):List[(Long,JobDesc)] = {
    
    val k = "job:context:" + uid
    val data = client.zrange(k, 0, -1)

    if (data.size() == 0) {
      null
    
    } else {
      
      data.map(record => {
        
        val Array(timestamp,jobdesc) = record.split(":")
        (timestamp.toLong,Serializer.deserializeJob(jobdesc))
        
      }).toList
      
    }
    
  }

}