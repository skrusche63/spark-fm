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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.actor.BaseTrainer

import de.kp.spark.fm.Configuration
import de.kp.spark.fm.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class FMBuilder(@transient val sc:SparkContext) extends BaseTrainer(Configuration) {
  
  override def train(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }

  override def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
    req.task.split(":")(1) match {
      
      case "matrix" => {
        /*
         * This is a training task that builds a correlation
         * matrix on top of a factorization or polynom model.
         * 
         * It must be ensured that the respective model the
         * previous model build task is finished
         */
        if (cache.statusExists(req) == false) {
          return Some(messages.TASK_DOES_NOT_EXIST(uid))   
        }
        
        if (cache.status(req) != FMStatus.TRAINING_FINISHED) {
          return Some(messages.TRAINING_NOT_FINISHED(uid)) 
        }
        
      }
      
      case "model" => {
        /*
         * This is the starting point for all training tasks that
         * refer to an already existing factorization or polynom
         * model
         */
        if (cache.statusExists(req)) {            
          return Some(messages.TASK_ALREADY_STARTED(uid))   
        }
    
        req.data.get(Names.REQ_SOURCE) match {
        
          case None => return Some(messages.NO_SOURCE_PROVIDED(uid))       
        
          case Some(source) => {
            if (Sources.isSource(source) == false) {
              return Some(messages.SOURCE_IS_UNKNOWN(uid,source))    
            }          
          
          }
        
        }
        
      }
      
      case _ => return Some(messages.TASK_IS_UNKNOWN(uid, req.task))
    
    }

    None
    
  }

  override def actor(req:ServiceRequest):ActorRef = {
    
    req.task.split(":")(1) match {
      
      case "matrix" => context.actorOf(Props(new MatrixActor(sc)))
      case "model"  => context.actorOf(Props(new ModelActor(sc)))
      
      case _ => null
      
    }
    
  }
  
}