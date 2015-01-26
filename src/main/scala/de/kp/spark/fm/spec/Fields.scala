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

import de.kp.spark.fm.Configuration

import scala.xml._
import scala.collection.mutable.HashMap

object Fields extends Serializable {
  
  val path = "features.xml"

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)

  def get(req:ServiceRequest):Map[String,String] = {

    val fields = HashMap.empty[String,String]
  
    try {
          
      if (cache.fieldsExist(req)) {      
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec) {
          fields += field.name -> field.value
          
        }
    
      } else {
        
        val root = XML.load(getClass.getClassLoader.getResource(path))     
        for (field <- root \ "field") {
      
          val name = (field \ "@name").toString
          val valu = field.text
          fields += name -> valu
      
        }
      
      }
      
    } catch {
      case e:Exception => {}
    }
    
    fields.toMap
    
  }

}

