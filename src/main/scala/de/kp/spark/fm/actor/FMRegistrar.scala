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

import de.kp.spark.core.model._
import de.kp.spark.fm.model._

import scala.collection.mutable.ArrayBuffer

class FMRegistrar extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {
      val origin = sender    
      val uid = req.data("uid")
      
      val response = try {
        
        /* Unpack fields from request and register in Redis instance */
        val fields = ArrayBuffer.empty[Field]

        /*
         * ********************************************
         * 
         *  "uid" -> 123
         *  "names" -> "target,feature,feature,feature"
         *
         * ********************************************
         * 
         * It is important to have the names specified in the order
         * they are used (later) to retrieve the respective data
         */
        val names = req.data("names").split(",")
        for (name <- names) {
          fields += new Field(name,"double","")
        }
 
        cache.addFields(req, new Fields(fields.toList))
        
        new ServiceResponse("context","register",Map("uid"-> uid),FMStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)

    }
    
  }

}