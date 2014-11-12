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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.Date

import de.kp.spark.fm.{Configuration,FM,FMModel,SparseVector}

import de.kp.spark.fm.model._
import de.kp.spark.fm.source.FeatureSource

import de.kp.spark.fm.redis.RedisCache

class FMActor(@transient val sc:SparkContext) extends BaseActor {
  
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
        RedisCache.addStatus(req,FMStatus.STARTED)
 
        try {
                   
          val dataset = new FeatureSource(sc).get(req.data)
          if (dataset != null) buildFM(req,dataset)
          
        } catch {
          case e:Exception => RedisCache.addStatus(req,FMStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def buildFM(req:ServiceRequest,dataset:RDD[(Int,(Double,SparseVector))]) {
    
    /* Update cache */
    RedisCache.addStatus(req,FMStatus.DATASET)

    /* Train polynom (model) */
    val (c,v,m) = FM.trainFromRDD(dataset,req.data)

    /* Determine error */
    val rsme = FM.calculateRSME(sc,req.data,c,v,m)

    val now = new Date()
    val dir = base + "/hmm-" + now.getTime().toString
    
    /* Save polynom in directory of file system */
    new FMModel(c,v,m,req.data).save(dir)
    
    /* Put polynom to cache */
    RedisCache.addPolynom(req,dir)
         
    /* Update cache */
    RedisCache.addStatus(req,FMStatus.FINISHED)
    
    /* Notify potential listeners */
    notify(req,FMStatus.FINISHED)
    
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

}