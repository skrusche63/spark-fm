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

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.fm._

class FMMaster(@transient ctx:RequestContext) extends BaseMaster(ctx.config) {
  
  protected def actor(worker:String):ActorRef = {
    
    worker match {
       
      case "fields"   => context.actorOf(Props(new FieldQuestor(ctx.config)))        
      case "register" => context.actorOf(Props(new BaseRegistrar(ctx.config)))
      
      case "index"    => context.actorOf(Props(new BaseIndexer(ctx.config)))        
      case "track"    => context.actorOf(Props(new BaseTracker(ctx.config)))

      case "params"   => context.actorOf(Props(new ParamQuestor(ctx.config)))

      case "status"   => context.actorOf(Props(new StatusQuestor(ctx.config)))
      case "train"    => context.actorOf(Props(new FMBuilder(ctx)))

      case "predict"  => context.actorOf(Props(new FMPredictor(ctx))) 
      case "similar"  => context.actorOf(Props(new FMSimilar(ctx))) 
       
      case _ => null
      
    }
  
  }

}