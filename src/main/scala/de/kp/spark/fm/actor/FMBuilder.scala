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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.actor.BaseTrainer

import de.kp.spark.fm._
import de.kp.spark.fm.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class FMBuilder(@transient val ctx:RequestContext) extends BaseTrainer(ctx.config) {
  
  override def train(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }

  override def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
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

    None
    
  }

  override def actor(req:ServiceRequest):ActorRef = context.actorOf(Props(new ModelActor(ctx)))
  
}