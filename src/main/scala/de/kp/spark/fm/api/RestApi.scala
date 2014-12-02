package de.kp.spark.fm.api
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

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}
import spray.routing.directives.CachingDirectives

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.spark.fm.Configuration
import de.kp.spark.fm.actor.FMMaster

import de.kp.spark.fm.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system
  
  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new FMMaster(sc)), name="context-master")
 
  private val service = "context"
    
  def start() {
    RestService.start(routes,system,host,port)
  }

  private def routes:Route = {

    path("admin" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doAdmin(ctx,subject)
	    }
	  }
    }  ~  
    path("get" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("index") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx)
	    }
	  }
    }  ~ 
    path("register") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx)
	    }
	  }
    }  ~ 
    path("track") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx)
	    }
	  }
    }  ~      
    path("train" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx,subject)
	    }
	  }
    }

  }
  
  private def doAdmin[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      /*
       * A 'fields' request suppors the retrieval of the field
       * or metadata specificiations that are associated with
       * a certain training task (uid).
       * 
       * The approach actually supported enables the registration
       * of field specifications on a per uid basis, i.e. each
       * task may have its own fields. Requests that have to
       * refer to the same fields must provide the SAME uid
       */
      case "fields" => doRequest(ctx,service,subject)
      /*
       * A 'status' request supports the retrieval of the status
       * with respect to a certain training task (uid). The latest
       * status or all stati of a certain task are returned.
       */
      case "status" => doRequest(ctx,service,subject)
      
      case _ => {}
      
    }
    
  }
  
  private def doGet[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      /*
       * A 'feature' based request supports target value prediction, i.e.
       * a feature vectoris provided with the request and the trained 
       * factorization model is used to determine the associated target
       */
      case "feature" => doRequest(ctx,service,"get:feature")
      /*
       * A 'similar' request is based on a trained correlation matrix
       * and determines the top 'k' most similar features to a set of
       * provided feature names
       */ 
      case "similar" => doRequest(ctx,service,"get:similar")
     
      case _ => {}
      
    }
    
  }
  
  private def doIndex[T](ctx:RequestContext) = doRequest(ctx,service,"index:feature")
  
  private def doRegister[T](ctx:RequestContext) = doRequest(ctx,service,"register")

  /**
   * 'train' requests initiate model building; this comprises either a correlation
   * matrix on top of a factorization model or a factorization model itself
   */
  private def doTrain[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      
      /* ../train/matrix */
      case "matrix" => {
        /*
         * This request trains a correlation matrix on top of a factorization
         * model; this matrix can be used to compute similarities between
         * certain specific features
         */
        doRequest(ctx,service,"train:matrix")
      }
      /* ../train/model */
      case "model" => {
        /*
         * This request trains a factorization model from a feature-based
         * dataset and saves this model in a pre-configured directory on
         * the file system
         */
        doRequest(ctx,service,"train:model")
        
      }
        
      case _ => {}
    }
    
    
  }

  private def doTrack[T](ctx:RequestContext) = doRequest(ctx,service,"track:feature")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String) = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}