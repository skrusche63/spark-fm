package de.kp.spark.fm.actor
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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.fm.model._
import de.kp.spark.fm.{Configuration,RemoteContext}

abstract class BaseActor extends Actor with ActorLogging {

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  
  protected def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,FMStatus.FAILURE)	
      
    } else {
      /*
       * The request data are also sent back to the requestor to enable
       * an appropriate evaluation of this response message 
       */
      val data = Map("message" -> message) ++ req.data
      new ServiceResponse(req.service,req.task,data,FMStatus.FAILURE)	
    
    }
    
  }

  /**
   * Notify all registered listeners about a certain status
   */
  protected def notify(req:ServiceRequest,status:String) {

    val response = new ServiceResponse(req.service,req.task,req.data,status)	
    val message = Serializer.serializeResponse(response)    

    RemoteContext.notify(message)
    
  }

  protected def serialize(resp:ServiceResponse) = Serializer.serializeResponse(resp)

}