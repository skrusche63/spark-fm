package de.kp.spark.fm.io
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

import java.util.concurrent.locks.ReentrantLock

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.index.IndexRequest.OpType

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.elasticsearch.client.Requests
import scala.collection.JavaConversions._

class ElasticWriter {

  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())
  private val indexCreationLock = new ReentrantLock()

  private var readyToWrite = false
  
  def open(index:String,mapping:String,builder:XContentBuilder):Boolean = {
        
    try {
      
      indexCreationLock.lock()
      val indices = client.admin().indices
      /*
       * Check whether referenced index exists; if index does not
       * exist, create one
       */
      val existsRes = indices.prepareExists(index).execute().actionGet()            
      if (existsRes.isExists() == false) {
        
        val createRes = indices.prepareCreate(index).execute().actionGet()            
        if (createRes.isAcknowledged() == false) {
          new Exception("Failed to create " + index)
        }
            
      }

      /*
       * Check whether the referenced mapping exists; if mapping
       * does not exist, create one
       */
      val prepareRes = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
      if (prepareRes.mappings().isEmpty) {

        val mappingRes = indices.preparePutMapping(index).setType(mapping).setSource(builder).execute().actionGet()
            
        if (mappingRes.isAcknowledged() == false) {
          new Exception("Failed to create mapping for " + index + "/" + mapping)
        }            

      }
      
      readyToWrite = true

    } catch {
      case e:Exception => {
        logger.error(e.getMessage())

      }
       
    } finally {
     indexCreationLock.unlock()
    }
    
    readyToWrite
    
  }

  def close() {
    if (node != null) node.close()
  }
    
  def write(index:String,mapping:String,source:java.util.Map[String,Object]):Boolean = {
    
    if (readyToWrite == false) return false
 		
    /* Update index operation */
    val content = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)
    content.map(source)
	
    client.prepareIndex(index, mapping).setSource(content).setRefresh(true).setOpType(OpType.INDEX)
      .execute(new ActionListener[IndexResponse]() {
        override def onResponse(response:IndexResponse) {
          /*
           * Registration of provided source successfully performed; no further
           * action, just logging this event
           */
          val msg = String.format("""Successful registration for: %s""", source.toString)
          logger.info(msg)
        
        }      

        override def onFailure(t:Throwable) {
	      /*
	       * In case of failure, we expect one or both of the following causes:
	       * the index and / or the respective mapping may not exists
	       */
          val msg = String.format("""Failed to register %s""", source.toString)
          logger.info(msg,t)
	      
          close()
          throw new Exception(msg)
	    
        }
        
      })
      
    true
  
  }
  
}