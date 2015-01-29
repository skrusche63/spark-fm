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

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.fm._
import de.kp.spark.fm.model._

class FMPredictor(@transient ctx:RequestContext) extends BaseActor {

  implicit val ec = context.dispatcher
  val sink = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)
      
      val response =         
        if (sink.modelExists(req) == false) {           
          failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
        } else {    
            
          /* 
           * Retrieve path to polynom: a model is uniquely determined
           * by the model name and the respective task identifier (uid);
           * this approach enables to train different models for the 
           * same uid
           */
          val path = sink.model(req)
          if (path == null) {
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
          } else {
              
            if (req.data.contains(Names.REQ_FEATURES)) {
              
              try {

                val (c,v,m,p,blocks) = FMUtil.read(path)
                val fm = new FM(ctx)
                  
                val features = req.data(Names.REQ_FEATURES).split(",").map(_.toDouble)
                val prediction = fm.predict(features, c, v, m, p).toString

                val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> prediction)
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
          
      origin ! response
      context.stop(self)
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }

}