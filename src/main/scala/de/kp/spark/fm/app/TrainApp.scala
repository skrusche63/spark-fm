package de.kp.spark.fm.app
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

import akka.actor._
import com.typesafe.config.ConfigFactory

import org.clapper.argot._

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.actor.Supervisor
import de.kp.spark.core.SparkService

import de.kp.spark.fm.{Configuration,RequestContext}

import de.kp.spark.fm.actor.FMMaster
import de.kp.spark.fm.model._

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.HashMap

object TrainApp extends SparkService {
  
  protected val sc = createCtxLocal("FactorizationContext",Configuration.spark)      
  protected val system = ActorSystem("FactorizationSystem")

  protected val inbox = Inbox.create(system)
  
  sys.addShutdownHook({
    /*
     * In case of a system shutdown, we also make clear
     * that the SparkContext is properly stopped as well
     * as the respective Akka actor system
     */
    sc.stop
    system.shutdown
    
  })
  
  def main(args:Array[String]) {
    
    try {
      
      val req_params = createParams(args)
      val req = new ServiceRequest("context","train:model",req_params)
      
      val ctx = new RequestContext(sc)
      val actor = system.actorOf(Props(new Handler(ctx)))   
      
      inbox.watch(actor)    
      actor ! req

      val timeout = DurationInt(req_params("timeout").toInt).minute
    
      while (inbox.receive(timeout).isInstanceOf[Terminated] == false) {}    
      sys.exit
      
    } catch {
      case e:Exception => {
          
        println(e.getMessage) 
        sys.exit
          
      }
    
    }
    
  }
  
  protected def createParams(args:Array[String]):Map[String,String] = {

    import ArgotConverters._
     
    val parser = new ArgotParser(
      programName = "Context-Aware Analysis Engine",
      compactUsage = true,
      preUsage = Some("Version %s. Copyright (c) 2015, %s.".format("1.0","Dr. Krusche & Partner PartG"))
    )

    val site = parser.option[String](List("key"),"key","Unique application key")
    val uid = parser.option[String](List("uid"),"uid","Unique job identifier")

    val name = parser.option[String](List("name"),"name","Unique job designator")

    val config = parser.option[String](List("config"),"config","Configuration file")
    parser.parse(args)

    /* Collect parameters */
    val params = HashMap.empty[String,String]
         
    /* Validate parameters */
    site.value match {
      
      case None => parser.usage("Parameter 'key' is missing.")
      case Some(value) => params += "site" -> value
    
    }
    
    uid.value match {
      
      case None => parser.usage("Parameter 'uid' is missing.")
      case Some(value) => params += "uid" -> value
      
    }
    
    name.value match {
      
      case None => parser.usage("Parameter 'name' is missing.")
      case Some(value) => params += "name" -> value
      
    }

    config.value match {
      
      case None => parser.usage("Parameter 'config' is missing.")
      case Some(value) => {
        
        val cfg = ConfigFactory.load(value)
        
        params += "algorithm" -> cfg.getString("algo")
        params += "source" -> cfg.getString("source")

        params += "init_mean" -> cfg.getDouble("init_mean").toString
        params += "init_stdev" -> cfg.getDouble("init_stdev").toString

        params += "num_partitions" -> cfg.getInt("num_partitions").toString
              
        params += "num_attribute" -> cfg.getInt("num_attribute").toString
        params += "num_factor" -> cfg.getInt("num_factor").toString

        params += "num_iter" -> cfg.getInt("num_iter").toString
        params += "learn_rate" -> cfg.getDouble("learn_rate").toString
       
        params += "k0" -> cfg.getBoolean("k0").toString
        params += "k1" -> cfg.getBoolean("k1").toString

        params += "reg_c" -> cfg.getDouble("reg_c").toString
        params += "reg_v" -> cfg.getDouble("reg_v").toString
        params += "reg_m" -> cfg.getDouble("reg_m").toString

      }
      
    }
    
    /* Add timestamp as global parameter */
    params += "timestamp" -> new java.util.Date().getTime.toString
    params.toMap
    
  }
  
}

class Handler(@transient ctx:RequestContext) extends Actor {
    
  private val config = Configuration
  def receive = {
    
    case req:ServiceRequest => {

      val start = new java.util.Date().getTime     
      println("Trainer started at " + start)
 
      val master = context.actorOf(Props(new FMMaster(ctx))) 
      master ! Serializer.serializeRequest(req)

      val status = FMStatus.TRAINING_FINISHED
      val supervisor = context.actorOf(Props(new Supervisor(req,status,config)))
       
    }
    
    case evt:StatusEvent => {
      /*
       * The StatusEvent message is returned from the
       * supervisor actor and specifies that the model
       * training task has been finished
       */
      val end = new java.util.Date().getTime           
      println("Trainer finished at " + end)
       
      context.stop(self)
      
    }

    case msg:String => {
    
      val end = new java.util.Date().getTime           
      println("Trainer finished at " + end)
    
      val response = Serializer.deserializeResponse(msg)
        
      println("Message: " + response.data("message").toString)
      println("Status: " + response.status)
      
    }
    
  }
  
}