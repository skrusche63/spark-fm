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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisCache

class FeatureHandler(req:ServiceRequest) {

  private val (host,port) = Configuration.redis
  private val cache = new RedisCache(host,port.toInt)

  private val fields = cache.fields(req)   
  private val zipped = fields.zipWithIndex.map(x => (x._2,x._1.name))

  /**
   * Determine index for the provided field names; this internal method
   * does not support explicit error handling and throws an exception
   * if fields for the provided names do not exist
   */  
  def columns:List[Int] = {
    
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
      zipped.filter(x => names.contains(x._2)).map(_._1).toList
    
    } else if (req.data.contains(Names.REQ_START) && req.data.contains(Names.REQ_END)) {

        val start = req.data(Names.REQ_START).toInt
        val end   = req.data(Names.REQ_END).toInt
        
        (Range(start, end+1)).toList
        
      } else {
      throw new Exception("Provided parameters do not permit any data processing.")
    }
   
    
  }

  def lookup = zipped.toMap
  
}