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

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.fm.Configuration

class FMMaster(@transient sc:SparkContext) extends BaseMaster(Configuration) {
  
  protected def actor(worker:String):ActorRef = {
    
    worker match {
       
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))        
      case "register" => context.actorOf(Props(new FMRegistrar()))
      
      case "index"    => context.actorOf(Props(new BaseIndexer(Configuration)))        
      case "track"    => context.actorOf(Props(new BaseTracker(Configuration)))

      case "params"   => context.actorOf(Props(new ParamQuestor(Configuration)))

      case "status"   => context.actorOf(Props(new StatusQuestor(Configuration)))
      case "train"    => context.actorOf(Props(new FMBuilder(sc)))

      case "predict"  => context.actorOf(Props(new FMPredictor(sc))) 
      case "similar"  => context.actorOf(Props(new FMSimilar(sc))) 
       
      case _ => null
      
    }
  
  }

}