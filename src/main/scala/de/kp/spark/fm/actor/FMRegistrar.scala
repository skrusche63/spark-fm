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
import de.kp.spark.core.actor.BaseRegistrar

import de.kp.spark.fm.Configuration
import scala.collection.mutable.ArrayBuffer

class FMRegistrar extends BaseRegistrar(Configuration) {
  
  override def buildFields(names:Array[String],types:Array[String]):List[Field] = {

    val fields = ArrayBuffer.empty[Field]
    
    for (name <- names) {
      fields += new Field(name,"double","")
    }

    fields.toList   
  
  }

}