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

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisClient

import scala.collection.JavaConversions._

class RedisSink {

  val client  = RedisClient()
  val service = "context"
  
  def addPolynom(req:ServiceRequest,model:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "polynom:context:" + req.data("uid")
    val v = "" + timestamp + ":" + model
    
    client.zadd(k,timestamp,v)
    
  }
  
  def polynomExists(uid:String):Boolean = {

    val k = "polynom:context:" + uid
    client.exists(k)
    
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

}