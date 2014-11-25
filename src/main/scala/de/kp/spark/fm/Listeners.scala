package de.kp.spark.fm
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

import scala.xml._

import scala.io.Source
import scala.collection.mutable.HashMap

import de.kp.spark.core.model.Listener
import scala.util.control.Breaks._

object Listeners {

  private val path = "listeners.xml"
  
  private val root:Elem = XML.load(getClass.getClassLoader.getResource(path))  
  private val services = HashMap.empty[String,Listener]
  
  load()
  
  private def load() {

    for (service <- root \ "service") {
      
      val name  = (service \ "@name").toString
      val timeout = (service \ "@timeout").toString.toInt

      val url = service.text
      
      services += name -> Listener(timeout,url)
      
    }

  }

  def get(listener:String):Listener = {
    
    if (services.contains(listener)) {
      services(listener)
      
    } else null
    
  }
  
  def names():List[String] = services.map(_._1).toList
  
}