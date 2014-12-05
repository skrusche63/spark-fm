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

import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.fm.Configuration
import de.kp.spark.fm.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class FMMaster(@transient sc:SparkContext) extends BaseActor {
  
  val (duration,retries,time) = Configuration.actor   
      
  implicit val ec = context.dispatcher
  implicit val timeout:Timeout = DurationInt(time).second

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(duration).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {

    /*
     * This request is initiated by the Akka API and supports Context-Aware Analysis
     * from within an Akka based environment, different to the REST API based request
     * additional de- and serialization must be performed.
     */
    case msg:String => {
	  	    
	  val origin = sender

	  val req = Serializer.deserializeRequest(msg)
	  val response = execute(req)
      
      response.onSuccess {
        case result => origin ! serialize(result)
      }
      response.onFailure {
        case result => origin ! serialize(failure(req,Messages.GENERAL_ERROR(req.data(Names.REQ_UID))))	      
	  }
      
    }
    /*
     *  This request is initiated by the REST API; this interface supports the same
     *  functionality as for the Akka API; the difference is that de- and serialization
     *  is not required.
     */  
    case req:ServiceRequest => {
	  	    
	  val origin = sender

	  val response = execute(req)
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req,Messages.GENERAL_ERROR(req.data(Names.REQ_UID)))	      
	  }
      
    }
 
    case _ => {

      val msg = Messages.REQUEST_IS_UNKNOWN()          
      log.error(msg)

    }
    
  }

  private def execute(req:ServiceRequest):Future[ServiceResponse] = {
	/*
	 * A request task specifies the genuine task and also 
	 * an accompanied subtask that describes the information
	 * element in more detail that is subject of the request
	 */
    val task = req.task.split(":")(0)
    ask(actor(task),req).mapTo[ServiceResponse]
    
  }
  
  private def actor(worker:String):ActorRef = {
    
    worker match {
       
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))        
      case "register" => context.actorOf(Props(new FMRegistrar()))
      
      case "index"    => context.actorOf(Props(new FMIndexer()))        
      case "track"    => context.actorOf(Props(new BaseTracker(Configuration)))

      case "status"   => context.actorOf(Props(new StatusQuestor(Configuration)))
      case "train"    => context.actorOf(Props(new FMBuilder(sc)))

      case "get"      => context.actorOf(Props(new FMQuestor(sc))) 
       
      case _ => null
      
    }
  
  }

}