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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.math.SMatrix

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.fm.FeatureHandler
import de.kp.spark.fm.model._

import scala.collection.JavaConversions._

class FMSimilar(@transient sc:SparkContext) extends BaseActor {

  implicit val ec = context.dispatcher
  val sink = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)
      
      val Array(task,topic) = req.task.split(":")
      val response = topic match {
       
        case "feature" => {
          /*
           * This request provides a certain feature index, e.g. for a user
           * or an item and returns the top most similar features
           */
          if (sink.matrixExists(req) == false) {
            failure(req,Messages.MATRIX_DOES_NOT_EXIST(uid))
            
          } else {
            
            try {
              /* 
               * Retrieve path to matrix: a matrix is uniquely determined
               * by the matrix name and the respective task identifier (uid);
               * this approach enables to train different matrices for the 
               * same uid
               */
              val path = sink.matrix(req)
              if (path == null) {
                failure(req,Messages.MATRIX_DOES_NOT_EXIST(uid))
              
              } else {
                
                val handler = new FeatureHandler(req)
                val similarity = loadMatrix(path)   
                
                val columns = handler.columns.sorted
                /*
                 * We have built the similarity matrix from the interaction 
                 * part (m) of the factorization or polynom model, i.e. we 
                 * compare features by their interaction with all other features.
                 * 
                 * To determine the respective SMatrix we use offsets for the 
                 * matrix positions
                 */
                val offset = columns(0)
                val indexes = columns.map(x => x - offset)     
                
                /*
                 * For all fields provided compute the top 'k' similar fields
                 */
                val total = req.data(Names.REQ_TOTAL).toInt
                val scores = indexes.map(index => (index,similarity.getHighest(index,total)))
                /*
                 * As a next step the (internal) column or feature index is re-mapped onto
                 * the external field name
                 */
                val lookup = handler.lookup                
                val similars = scores.map(x => {
                  
                  val field = lookup(x._1 + offset)
                  val related = x._2.map(v => {
                    
                    val name = lookup(v._1 + offset)
                    ScoredField(name,v._2)
                    
                  })
                  
                  SimilarFields(field,related)
                  
                })
                
                val result = Serializer.serializeSimilars(Similars(similars))
                val data = Map(Names.REQ_UID -> uid, topic -> result)
                  
                new ServiceResponse(req.service,req.task,data,FMStatus.SUCCESS)
                 
              }
           
            } catch {
              case e:Exception => {
                failure(req,e.toString())                   
              }
            }
              
          }
        }
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)          
          failure(req,msg)
           
        }
        
      }
          
      origin ! response
      context.stop(self)
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
  
  private def getBlock(req:ServiceRequest):List[Int] = {
    
    if (req.data.contains(Names.REQ_COLUMNS)) {
       /*
       * The feature block has to be determined from a list
       * of provided column positions
       */
      req.data(Names.REQ_COLUMNS).split(",").map(_.toInt).toList
  
    } else if (req.data.contains(Names.REQ_FIELDS)) {
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
  
  private def loadMatrix(path:String):SMatrix = {
    
    val file = sc.textFile(path).coalesce(1,false)
    val dim = file.count().toInt

    def seqOp(matrix:SMatrix, data:String):SMatrix = {      
      matrix.deserializeRow(data)
      matrix
    }
    /* Note that matrix1 is always NULL */
    def combOp(matrix1:SMatrix,matrix2:SMatrix):SMatrix = matrix2      

    file.aggregate(new SMatrix(dim,1))(seqOp,combOp)    

  }
  
}