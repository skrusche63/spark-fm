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

import de.kp.spark.fm.FMModel

import de.kp.spark.fm.model._
import de.kp.spark.fm.redis.RedisCache

import scala.collection.JavaConversions._

class FMQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {
        
        case "get:prediction" => {
          /*
           * This request retrieves a set of features and computes
           * the target (or decision) variable 
            */
          val resp = if (RedisCache.polynomExists(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            /* Retrieve path to polynom for 'uid' from cache */
            val path = RedisCache.polynom(uid)
            if (path == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {
              
              if (req.data.contains("features")) {
              
                try {
                
                  val model = new FMModel()
                  model.load(path)
                  
                  val prediction = model.predict(req.data("features").split(",").map(_.toDouble)).toString

                  val data = Map("uid" -> uid, "prediction" -> prediction)
                  new ServiceResponse(req.service,req.task,data,FMStatus.SUCCESS)
                
                } catch {
                  case e:Exception => {
                    failure(req,e.toString())                   
                  }
                }
                
              } else {
                failure(req,Messages.MISSING_FEATURES(uid))
                
              }
            }
          }
             
          origin ! Serializer.serializeResponse(resp)
          
        }
                 
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! Serializer.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,FMStatus.FAILURE)	
    
  }
  
}