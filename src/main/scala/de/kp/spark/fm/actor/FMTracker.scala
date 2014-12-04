package de.kp.spark.fm.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-FM project
 * (https://github.com/skrusche63/spark-cluster).
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

import de.kp.spark.core.Names
import de.kp.spark.core.actor.BaseTracker

import de.kp.spark.fm.Configuration

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class FMTracker extends BaseTracker(Configuration) {
  
  override def prepareFeature(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val now = new java.util.Date()
    val source = HashMap.empty[String,String]
    
    source += Names.SITE_FIELD -> params(Names.SITE_FIELD)
    source += Names.TIMESTAMP_FIELD -> now.getTime().toString    
 
    /* 
     * Restrict parameters to those that are relevant to feature description;
     * note, that we use a flat JSON data structure for simplicity and distinguish
     * field semantics by different prefixes 
     */
    val records = params.filter(kv => kv._1.startsWith("lbl.") || kv._1.startsWith("fea."))
    for (rec <- records) {
      
      val (k,v) = rec
        
      val name = k.replace("lbl.","").replace("fea.","")
      source += k -> v      
      
    }

    source
    
  }
 
}