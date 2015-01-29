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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.source._

import de.kp.spark.fm._
import de.kp.spark.fm.model._

import de.kp.spark.fm.spec.{Fields}

/**
 * TargetedPointSource is an abstraction layer on top of the physical 
 * data sources supported by FM; the data format is that of a 
 * targeted point, which combines a feature (Double) vector of 
 * variables and a target (Double) variable
 */
class TargetedPointSource(@transient ctx:RequestContext) {

  private val config = ctx.config
  private val model = new TargetedPointModel(ctx)
  
  def get(req:ServiceRequest):(Seq[FMBlock],RDD[(Double,FMVector)]) = {
 
    val source = req.data(Names.REQ_SOURCE)
    source match {

      case Sources.ELASTIC => {
       
       val rawset = new ElasticSource(ctx.sc).connect(config,req)
       model.buildElastic(req,rawset)
       
      }
 
      case Sources.FILE => {
       
        val rawset = new FileSource(ctx.sc).connect(config.input(0),req)
        model.buildFile(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2).toList  
        
        val rawset = new JdbcSource(ctx.sc).connect(config,req,fields)
        model.buildJDBC(req,rawset)

      }

      case Sources.PARQUET => {
        
        val rawset = new ParquetSource(ctx.sc).connect(config.input(0),req)
        model.buildParquet(req,rawset)

      }
      
      case _ => null
      
    }

  }
  
}