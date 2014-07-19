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

import org.apache.spark.{RangePartitioner,SparkConf,SparkContext}
import org.apache.spark.SparkContext._

class SparkHelper {

  def partitionTextFile(sc:SparkContext, partitions:Int, input:String, output:String) {

    /**
     * This is a simple mechanism to partition input data
     * into a set of almost equal sized datasets
     */
    val file = sc.textFile(input).map(line => {
      val ix = new java.util.Random().nextInt(partitions)
      (ix,line)
    })
    
    val lines = file.partitionBy(new RangePartitioner(partitions,file))
    for (i <- 0 until partitions) {
      
      val data = lines.filter(line => line._1 == i).map(line => line._2)
      data.saveAsTextFile(output + "/" + i)
      
    }

  }
  
}