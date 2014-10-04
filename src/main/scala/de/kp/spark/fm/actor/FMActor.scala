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

import akka.actor.Actor
import org.apache.spark.rdd.RDD

import de.kp.spark.fm.{Configuration,FM,FMModel,SparseVector}

import de.kp.spark.fm.model._
import de.kp.spark.fm.source.FeatureSource

import de.kp.spark.fm.redis.RedisCache

class FMActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("FMActor",Configuration.spark)      
  
  private val base = Configuration.model
  
  def receive = {

    case req:ServiceRequest => {

      val uid = req.data("uid")
      val task = req.task
      
      val missing = properties(req)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(uid,task,FMStatus.STARTED)
 
        try {
                   
          val dataset = new FeatureSource(sc).get(req.data)
          if (dataset != null) buildFM(uid,task,dataset,req.data)
          
        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,FMStatus.FAILURE)          
        }
 

      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
      context.stop(self)
      
    }
    
  }
  
  private def buildFM(uid:String,task:String,dataset:RDD[(Int,(Double,SparseVector))],params:Map[String,String]) {

    /* Update cache */
    RedisCache.addStatus(uid,task,FMStatus.DATASET)

    /* Train polynom (model) */
    val (c,v,m) = FM.trainFromRDD(dataset,params)

    /* Determine error */
    val rsme = FM.calculateRSME(sc,params,c,v,m)

    val now = new Date()
    val dir = base + "/hmm-" + now.getTime().toString
    
    /* Save polynom in directory of file system */
    new FMModel(c,v,m,params).save(dir)
    
    /* Put polynom to cache */
    RedisCache.addPolynom(uid,dir)
         
    /* Update cache */
    RedisCache.addStatus(uid,task,FMStatus.FINISHED)
    
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
      
      val k = req.data("k").toInt
      val minconf = req.data("minconf").toDouble
        
      return true
        
    } catch {
      case e:Exception => {
         return false          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,FMStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.FM_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,FMStatus.STARTED)	
  
    }

  }
}