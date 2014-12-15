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
     * A 'params' request supports the retrieval of the parameters
     * used for a certain model training task
     */
    path("params") {  
	  post {
	    respondWithStatus(OK) {
	      ctx => doParams(ctx)
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
   * have the same format as the vectors used in the training dataset.
   * 
   * Request parameters for the 'predict' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - features (String, comma-separated list of Doubles)
   * 
   */
  private def doPredict[T](ctx:RequestContext) = doRequest(ctx,service,"predict:feature")
  /**
   * 'similar' requests refer to the retrieval of features that are
   * most similar to a set of provided features; note, that these
   * features must be subset of those, that have been used to build
   * the correlation matrix.
   * 
   * Request parameters for the 'similar' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - total (String)
   * - columns (String, comma-separated list of Integers)
   * 
   * or 
   * 
   * - start (Integer)
   * - end (Integer)
   * 
   */
  private def doSimilar[T](ctx:RequestContext) = doRequest(ctx,service,"similar:feature")

  /**
   * 'fields' and 'register' requests refer to the metadata management; 
   * for a certain task (uid) and a specific model (name), a specification 
   * of the respective data fields can be registered and retrieved from a 
   * Redis database. Request parameters for the 'fields' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doFields[T](ctx:RequestContext) = doRequest(ctx,service,"fields:feature")
  /**
   * Request parameters for the 'register' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - names (String, comma separated list of feature names)
   * - types (String, comma separated list of feature types)
   * 
   */    
  private def doRegister[T](ctx:RequestContext) = doRequest(ctx,service,"register:feature")

  /**
   * 'index' and 'track' reflect the interface to the tracking functionality.
   * 
   * Request parameters for the 'index' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - source (String)
   * - type (String)
   * 
   * - names (String, comma separated list of feature names)
   * - types (String, comma separated list of feature types)
   * 
   */
  private def doIndex[T](ctx:RequestContext) = doRequest(ctx,service,"index:feature")
  /**
   * Request parameters for the 'track' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - source (String)
   * - type (String)
   * 
   * - lbl. xxx (Double, target value)
   * - fea. xxx (Double, predictor value) 
   * 
   */  
  private def doTrack[T](ctx:RequestContext) = doRequest(ctx,service,"track:feature")
  /**
   * Request parameters for the 'params' request:
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   */
  private def doParams[T](ctx:RequestContext) = doRequest(ctx,service,"params")

  /**
   * 'status' is an administration request to determine whether a certain model
   * or matrix building task has been finished or not. Request parameters for 
   * the 'status' request
   * 
   * - site (String)
   * - uid (String)
   * 
   */
  private def doStatus[T](ctx:RequestContext,subject:String) = {
    
    val task = "status:" + subject
    /*
     * The following topics are supported:
     * 
     * Retrieve the 'latest' status information about a certain
     * data mining or model building task.
     * 
     * Retrieve 'all' stati assigned to a certain data mining
     * or model building task.
     * 
     */
    val topics = List("latest","all")
    if (topics.contains(subject)) doRequest(ctx,service,task)
  
  }
  /**
   * 'train' requests initiate model building; this comprises either a correlation
   * matrix on top of a factorization model or a factorization model itself.
   *
   * Request parameters for the 'train' request
   * 
   * - site (String)
   * - uid (String)
   * - name (String)
   * 
   * - source (String, ELASTIC, FILE, JDBC, PARQUET)
   * 
   * and the following parameters depend on the selected source:
   * 
   * ELASTIC:
   * 
   * - source.index (String)
   * - source.type (String)
   * - query (String)
   * 
   * JDBC:
   * 
   * - query (String)
   * 
   * and the matrix building parameters are
   * 
   * - columns (String, comma-separated list of Integers)
   * 
   * or 
   * 
   * - start (Integer)
   * - end (Integer)
   * 
   * and the model building parameters are
   * 
   * - num_attribute (Integer)
   * - num_factor (Integer)
   * 
   * - num_iter (Integer)
   * - num_partitions (Integer)
   * 
   * - learn_rate (Double)
   * 
   * - init_mean (Double)
   * - init_stdev (Double)
   * 
   * - k0 (Boolean)
   * - k1 (Boolean)
   * 
   * - reg_c (Double)
   * - reg_v (Double)
   * - reg_m (Double)
   * 
   */
  private def doTrain[T](ctx:RequestContext,subject:String) = {
    
    val task = "train:" + subject
    /*
     * The following topics are supported:
     * 
     * 'matrix': This request trains a correlation matrix on 
     * top of a factorization model; this matrix can be used 
     * to compute similarities between certain specific features
     * 
     * 'model': This request trains a factorization model from a
     * feature-based dataset and saves this model in a pre-configured 
     * directory on the file system
     */
    val topics = List("matrix","model")
    if (topics.contains(subject)) doRequest(ctx,service,task)
    
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