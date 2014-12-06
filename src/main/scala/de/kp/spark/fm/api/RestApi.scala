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
    /*
     * A 'fields' request supports the retrieval of the field
     * or metadata specificiations that are associated with
     * a certain training task (uid).
     * 
     * The approach actually supported enables the registration
     * of field specifications on a per uid basis, i.e. each
     * task may have its own fields. Requests that have to
     * refer to the same fields must provide the SAME uid
     */
    path("fields") {  
	  post {
	    respondWithStatus(OK) {
	      ctx => doFields(ctx)
	    }
	  }
    }  ~  
    /*
     * A 'register' request supports the registration of a field
     * or metadata specification that describes the fields used
     * to span the training dataset.
     */
    path("register") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx)
	    }
	  }
    }  ~ 
    /*
     * 'index' and 'track' requests refer to the tracking functionality
     * of the Context-Aware Analysis engine; while 'index' prepares a
     * certain Elasticsearch index, 'track' is used to gather training
     * data.
     */
    path("index") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx)
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
    /*
     * A 'predict' request type supports target value prediction, i.e.
     * a feature vector is provided with the request and the trained 
     * factorization model is used to determine the associated target
     */
    path("predict") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doPredict(ctx)
	    }
	  }
    }  ~ 
    /*
     * A 'similar' request type supports the retrieval of the top most
     * similar features with respect to a set of provided features;
     * note, that the features provided must coincide with the data
     * structure of the feature vectors used in the training dataset.
     */
    path("similar") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doSimilar(ctx)
	    }
	  }
    }  ~ 
    /*
     * A 'status' request supports the retrieval of the status
     * with respect to a certain training task (uid). The latest
     * status or all stati of a certain task are returned.
     */
    path("status" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx,subject)
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
  /**
   * 'predict' requests refer to the retrieval of a predicted target
   * variable for a certain feature vector; the respective vector must
   * have the same format as the vectors used in the training dataset
   */
  private def doPredict[T](ctx:RequestContext) = doRequest(ctx,service,"predict:feature")
  /**
   * 'similar' requests refer to the retrieval of features that are
   * most similar to a set of provided features; note, that these
   * features must be subset of those, that have been used to build
   * the correlation matrix.
   */
  private def doSimilar[T](ctx:RequestContext) = doRequest(ctx,service,"similar:feature")

  /**
   * 'fields' and 'register' requests refer to the metadata management 
   * of the Context-Aware Analysis engine; for a certain task (uid) and 
   * a specific model (name), a specification of the respective data fields 
   * can be registered and retrieved from a Redis database.
   */
  private def doFields[T](ctx:RequestContext) = doRequest(ctx,service,"fields:feature")
  
  private def doRegister[T](ctx:RequestContext) = doRequest(ctx,service,"register:feature")

  /**
   * 'index' and 'track' reflect the interface to the tracking functionality
   * of the Context-Aware Analysis engine.
   */
  private def doIndex[T](ctx:RequestContext) = doRequest(ctx,service,"index:feature")

  private def doTrack[T](ctx:RequestContext) = doRequest(ctx,service,"track:feature")

  /**
   * 'status' is an administration request to determine whether a certain model
   * or matrix building task has been finished or not; the only parameter required 
   * for status requests is the unique identifier of a certain task
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    subject match {
      /*
       * Retrieve the 'latest' status information about a certain
       * data mining or model building task.
       */
      case "latest" => doRequest(ctx,service,"status:latest")
      /*
       * Retrieve 'all' stati assigned to a certain data mining
       * or model building task.
       */
      case "all" => doRequest(ctx,service,"status:all")
      
      case _ => {/* do nothing */}
    
    }
  
  }

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