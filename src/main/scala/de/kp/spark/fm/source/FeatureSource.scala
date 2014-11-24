package de.kp.spark.fm.source
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.fm.{Configuration,SparseVector}
import de.kp.spark.fm.model._

import de.kp.spark.fm.spec.{Fields}

/**
 * FeatureSource is an abstraction layer on top of the physical 
 * data sources supported by FM; the data format is that of a 
 * targeted point, which combines a feature (Double) vector of 
 * variables and a target (Double) variable
 */
class FeatureSource(@transient sc:SparkContext) {

  private val model = new FeatureModel(sc)
  
  def get(data:Map[String,String]):RDD[(Int,(Double,SparseVector))] = {
        
    val uid = data("uid")
    val partitions = data("num_partitions").toInt

    val source = data("source")
    source match {

      case Sources.FILE => {

        val path = Configuration.file()
        
        val rawset = new FileSource(sc).connect(data,path)
        model.buildFile(uid,rawset,partitions)
        
      }

      case Sources.ELASTIC => {
       
       val rawset = new ElasticSource(sc).connect(data)
       model.buildElastic(uid,rawset,partitions)
       
      }
 
      case Sources.JDBC => {

        val fields = Fields.get(uid)
        
        val rawset = new JdbcSource(sc).connect(data,fields)
        model.buildJDBC(uid,rawset,partitions)

      }
      
      case _ => null
      
    }

  }
  
}