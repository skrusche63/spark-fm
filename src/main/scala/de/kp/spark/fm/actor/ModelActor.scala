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

import java.util.Date

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.core.source.PointSource

import de.kp.spark.fm._

import de.kp.spark.fm.model._
import de.kp.spark.fm.spec.PointSpec

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}


class ModelActor(@transient ctx:RequestContext) extends BaseActor {
  
  private val base = ctx.config.model
  val sink = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val missing = (properties(req) == false)
      sender ! response(req, missing)

      if (missing == false) {
 
        try {
          train(req)    
          
        } catch {
          case e:Exception => cache.addStatus(req,FMStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def train(req:ServiceRequest) {

    /* Register status */
    cache.addStatus(req,FMStatus.TRAINING_STARTED)
    
    val (c,v,m) = buildPolynom(req)
    buildMatrix(req,m)
    
    /* Update cache */
    cache.addStatus(req,FMStatus.TRAINING_FINISHED)
    
  }
  
  private def buildPolynom(req:ServiceRequest):(Double,DenseVector,DenseMatrix) = {
   
    /*
     * The training request must provide a name for the factorization or 
     * polynom model to uniquely distinguish this model from all others
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for factorization model provided.")

    /*
     * STEP #1: Retrieve targeted data points from specified data source;
     * note, that the recommended data source is parquet
     */
    val source = new PointSource(ctx.sc,ctx.config,PointSpec)
    val (blocks,points) = FMFormatter.format(source.connect(req))
    /*
     * STEP #2: Train factorization machine and retrieve the respective
     * polynom coefficients, c, v, m
     */
    val fm = new FM(ctx)
    val (c,v,m) = fm.trainFromRDD(points,req.data)
    /*
     * STEP #3: Calculate the root mean sequred error RMSE
     */
    val rmse = fm.calculateRMSE(req.data, c, v, m, points)
    /*
     * STEP #4: Save the factorization machine model (polynom)
     * on the file system and put respective store to REDIS
     */
    val now = new Date()
    val store = String.format("""%s/model/%s/%s""",base,name,now.getTime().toString)
    
    val p = req.data
   
    FMUtil.writeModel(store, c, v, m, p, rmse, blocks)    
    sink.addModel(req,store)
    /*
     * Return polynom parameters to enable similarity 
     * matrix computation
     */
    (c,v,m)
    
  }
  
  /**
   * The interaction matrix is a (factor x features) matrix, where each row
   * specifies a certain latent factor and a column determines the engagement
   * with these latent factors; we there need to compute the column similarity 
   */
  private def buildMatrix(req:ServiceRequest,interaction:DenseMatrix) {
   
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for factorization model provided.")

    /*
     * STEP #1: Transform interaction matrix into RowMatrix
     * and calculate column similarity
     */
    val rows = (0 until interaction.rdim).map(i => Vectors.dense(interaction.getRow(i)))
    val mat = new RowMatrix(ctx.sc.parallelize(rows))

    val sim = mat.columnSimilarities()
    
    val rdim = sim.numRows.toInt
    val cdim = sim.numCols.toInt
    
    require (rdim == cdim)
    /*
     * STEP #2: Transform similarities into DenseMatrix to optimize
     * usage of similarity data
     */
    val matrix = new DenseMatrix(rdim,cdim)
    sim.entries.collect().foreach{case MatrixEntry(i,j,u) => matrix.update(i.toInt,j.toInt,u)}    
    /*
     * STEP #3: Serialize and save correlation matrix; the base 
     * path extended by matrix name and current timestamp
     */
    val now = new java.util.Date()
    val store = String.format("""%s/matrix/%s/%s""",base,name,now.getTime().toString)
    
    FMUtil.writeMatrix(store,matrix)
    sink.addMatrix(req,store)
     
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
        
      return true
        
    } catch {
      case e:Exception => {
         return false          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map("message" -> Messages.MISSING_PARAMETERS(uid)) ++ req.data
      new ServiceResponse(req.service,req.task,data,FMStatus.FAILURE)	
  
    } else {
      val data = Map("message" -> Messages.TRAINING_STARTED(uid)) ++ req.data
      new ServiceResponse(req.service,req.task,data,FMStatus.TRAINING_STARTED)	
  
    }

  }

}