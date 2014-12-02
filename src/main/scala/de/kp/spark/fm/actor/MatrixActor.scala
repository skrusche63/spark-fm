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

import de.kp.spark.core.Names
import de.kp.spark.core.math.{CosineSimilarity,SMatrix}

import de.kp.spark.core.model._

import de.kp.spark.fm.{Configuration,DenseMatrix,FMModel}

import de.kp.spark.fm.model._
import de.kp.spark.fm.sink.RedisSink

/**
 * The MatrixActor initiates the training of a correlation or
 * similarity matrix that is built from the feature interaction
 * computed by the factorization or polynom model
 */
class MatrixActor(@transient sc:SparkContext) extends BaseActor {
  
  val sink = new RedisSink()  
  private val base = Configuration.model
 
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      val response = if (sink.modelExists(req) == false) {           
        failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
      } else {    
          
        val data = Map(Names.REQ_UID -> uid, "message" -> Messages.MATRIX_TRAINING_STARTED(uid))
        new ServiceResponse(req.service,req.task,data,FMStatus.TRAINING_STARTED)	
      
      }
      
      origin ! response
      
      try {
        train(req)
      
      } catch {
        case e:Exception => cache.addStatus(req,FMStatus.FAILURE)          
      }
          
      context.stop(self)
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }

  private def train(req:ServiceRequest) {
    
    /**
     * The training request must provide a name for the correlation 
     * matrix to uniquely distinguish this matrix from all others
     */
    val name = if (req.data.contains(Names.REQ_NAME)) req.data(Names.REQ_NAME) 
      else throw new Exception("No name for matrix model provided.")
    
    /* Retrieve path to polynom for 'uid' from sink */
    val path = sink.model(req)
    if (path == null) {
      throw new Exception("Model does not exist.")
    }

    cache.addStatus(req,FMStatus.TRAINING_STARTED)

    val model = new FMModel()
    model.load(path)
    
    /* Retrieve factors from model */
    val (c,v,m,p) = model.factors
            
    /* 
     * Build correlation matrix: Retrieve all field names that
     * refer to a certain data feature; note, that factorization
     * machines use a binary representation and therefor a certain
     * data feature covers a block of field names.
     * 
     * These field names have to be provided with the request; this
     * approach is choosen to keep the Context-Analysis engine as
     * generic as possible  
     */
    val block = getBlock(req)
    
    /*
     * We build a similarity matrix from the interaction part (m) of
     * the factorization or polynom model, i.e. we compare features
     * by their interaction with all other features.
     * 
     * To determine the respective SMatrix we use offsets for the 
     * matrix positions
     */
    val offset = block(0)
    
    val indexes = block.map(x => x - offset)     
    val matrix = buildMatrix(indexes,m)

    /*
     * Serialize and save correlation matrix; the base path extended
     * by matrix name and current timestamp
     */
    val now = new java.util.Date()
    val dir = String.format("""%s/matrix/%s/%s""",base,name,now.getTime().toString)
    
    val output = matrix.serialize()
    sc.parallelize(output,1).saveAsTextFile(dir)
    
    /* Put offset and path of matrix to Redis sink */
    sink.addMatrix(req,dir)
         
    /* Update cache */
    cache.addStatus(req,FMStatus.TRAINING_FINISHED)
    
    /* Notify potential listeners */
    notify(req,FMStatus.TRAINING_FINISHED)
    
  }
  
  /**
   * Determine index for the provided field names; this internal method
   * does not support explicit error handling and throws an exception
   * if fields for the provided names do not exist
   */
  private def getBlock(req:ServiceRequest):List[Int] = {
    
    if (req.data.contains(Names.REQ_FIELDS)) {
      /*
       * The feature block has to be determined from a list
       * of provided field names
       */
      val names = req.data(Names.REQ_FIELDS).split(",").toList
      /*
       * As a next step the (internal) column or feature index is retrieved;
       * to this end, that field specification must be used from the cache
       */
      val fields = cache.fields(req)     
      val zipped = fields.zipWithIndex.map(x => (x._2,x._1.name))
     
      zipped.filter(x => names.contains(x._2)).map(_._1).toList
    
    } else if (req.data.contains(Names.REQ_START) && req.data.contains(Names.REQ_END)) {

      val start = req.data(Names.REQ_START).toInt
      val end   = req.data(Names.REQ_END).toInt
      
      Range(start,end+1).toList
      
    } else {
      throw new Exception("Provided parameters do not permit any data processing.")
    }
   
    
  }
  
  private def buildMatrix(indexes:List[Int],interaction:DenseMatrix):SMatrix = {
    /* 
     * The diagonal value (fixed for an SMatrix) is set to '1'
     * as we use cosine similarity measue
     */
    val dim = indexes.length
    val matrix = new SMatrix(dim,1)

    (0 until dim).foreach(i => {
      ((i+1) until dim).foreach(j => {
        
        val row_i = interaction.getRow(i)
        val row_j = interaction.getRow(j)
      
        val sim = CosineSimilarity.compute(row_i,row_j)
        matrix.set(i,j,sim)
        
      })
      
    })

    matrix
    
  }
  
}