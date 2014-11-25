package de.kp.spark.fm.spec
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
import de.kp.spark.core.redis.RedisCache

import scala.xml._
import scala.collection.mutable.ArrayBuffer

object Fields extends Serializable {
  
  val path = "features.xml"
  val cache = new RedisCache()

  def get(req:ServiceRequest):List[String] = {

    val fields = ArrayBuffer.empty[String]
  
    try {
          
      if (cache.fieldsExist(req)) {   
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec.items) {
          fields += field.name
        }
    
      } else {
        
        val root = XML.load(getClass.getClassLoader.getResource(path))  
        for (field <- root \ "field") {
           fields += field.text 
        }
      
     }
   
      
    } catch {
      case e:Exception => {}
    }
    
    fields.toList
    
  }

}

