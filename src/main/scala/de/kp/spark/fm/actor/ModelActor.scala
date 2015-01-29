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

import java.util.Date

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.fm._

import de.kp.spark.fm.model._
import de.kp.spark.fm.source.TargetedPointSource

class ModelActor(@transient ctx:RequestContext) extends BaseActor {
  
  private val base = ctx.config.model
  val sink = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val missing = (properties(req) == false)
      sender ! response(req, missing)

      if (missing == false) {
 
        try {
          train(req)    
          
        } catch {
          case e:Exception => cache.addStatus(req,FMStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def train(req:ServiceRequest) {

    /*
     * The training request must provide a name for the factorization or 
     * polynom model to uniquely distinguish this model from all others
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for factorization model provided.")

    /* Register status */
    cache.addStatus(req,FMStatus.MODEL_TRAINING_STARTED)
                   
    val (blocks,points) = new TargetedPointSource(ctx).get(req)

    val fm = new FM(ctx)
    /* Train polynom (model) */
    val (c,v,m) = fm.trainFromRDD(points,req.data)

    /* Determine error */
    val rsme = fm.calculateRSME(req,c,v,m,points)
    
    val now = new Date()
    val store = String.format("""%s/model/%s/%s""",base,name,now.getTime().toString)
    
    /* Save polynom in directory of file system */
    val p = req.data
    FMUtil.writeModel(store, c, v, m, p, blocks)
    
    /* Put path to polynom to Redis sink */
    sink.addModel(req,store)
         
    /* Update cache */
    cache.addStatus(req,FMStatus.MODEL_TRAINING_FINISHED)
    
    /* Notify potential listeners */
    notify(req,FMStatus.MODEL_TRAINING_FINISHED)
    
  }
  
  private def properties(req:ServiceRequest):Boolean = {
      
    try {
      
      if (req.data.contains("init_mean") == false) return false
      if (req.data.contains("init_stdev") == false) return false
      
      if (req.data.contains("num_partitions") == false) return false
      
      if (req.data.contains("num_attribute") == false) return false
      if (req.data.contains("num_factor") == false) return false
      
      if (req.data.contains("num_iter") == false) return false
      if (req.data.contains("learn_rate") == false) return false
       
      if (req.data.contains("k0") == false) return false
      if (req.data.contains("k1") == false) return false
        
      if (req.data.contains("reg_c") == false) return false
      if (req.data.contains("reg_v") == false) return false
      if (req.data.contains("reg_m") == false) return false
        
      return true
        
    } catch {
      case e:Exception => {
         return false          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map("message" -> Messages.MISSING_PARAMETERS(uid)) ++ req.data
      new ServiceResponse(req.service,req.task,data,FMStatus.FAILURE)	
  
    } else {
      val data = Map("message" -> Messages.MODEL_TRAINING_STARTED(uid)) ++ req.data
      new ServiceResponse(req.service,req.task,data,FMStatus.MODEL_TRAINING_STARTED)	
  
    }

  }

}