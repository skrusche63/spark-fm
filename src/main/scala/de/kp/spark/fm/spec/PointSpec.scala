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

import de.kp.spark.core.spec.Fields
import de.kp.spark.fm.Configuration

import scala.xml._
import scala.collection.mutable.Buffer

class PointSpec(req:ServiceRequest) extends Fields {
  
  val path = "features.xml"

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)

  private val fields = load
  
  def mapping:Map[String,String] = fields.map(x => (x.name,x.value)).toMap

  def names:List[String] = fields.map(_.name)
  
  def types:List[String] = fields.map(_.datatype)
  
  private val load:List[Field] = {
    
    val data = Buffer.empty[Field]
    
    try {
          
      if (cache.fieldsExist(req)) {   
        
        val fieldspec = cache.fields(req)
        for (field <- fieldspec) {
          data += Field(field.name,field.datatype,field.value)
        }
        
      } else {

        val root = XML.load(getClass.getClassLoader.getResource(path))     
        for (field <- root \ "field") {
      
          val _name  = (field \ "@name").toString
          val _type  = (field \ "@type").toString

          val _mapping = field.text
          
          data += Field(_name,_type,_mapping)
      
        }
      
     }
      
    } catch {
      case e:Exception => {}
    }
    
    data.toList
    
  } 

}

