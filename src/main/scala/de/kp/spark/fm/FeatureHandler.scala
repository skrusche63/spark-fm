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

class FeatureHandler(req:ServiceRequest) {

  def columns:List[Int] = {
    
    if (req.data.contains(Names.REQ_COLUMNS)) {
       /*
       * The feature block has to be determined from a list
       * of provided column positions
       */
      req.data(Names.REQ_COLUMNS).split(",").map(_.toInt).toList
    
    } else if (req.data.contains(Names.REQ_START) && req.data.contains(Names.REQ_END)) {

        val start = req.data(Names.REQ_START).toInt
        val end   = req.data(Names.REQ_END).toInt
        
        (Range(start, end+1)).toList
        
      } else {
      throw new Exception("Provided parameters do not permit any data processing.")
    }
   
    
  }
  
}