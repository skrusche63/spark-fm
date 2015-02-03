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

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.serializer.KryoSerializer

object ExampleApp {
  
  def main(args:Array[String]) {

    /**
     * Example settings
     */
    val args = Map(
      
      "dataset" -> "/Work/tmp/fm/input.txt",  
        
      "init_mean"  -> "0.0",
      "init_stdev" -> "0.01",
      
      "num_partitions" -> "100",
      
      "num_attribute" -> "8",
      "num_factor"    -> "10",
      
      "num_iter"   -> "100",
      "learn_rate" -> "0.1",
       
      "k0" -> "true",
      "k1" -> "true",
        
      "reg_c" -> "0.0",
      "reg_v" -> "0.0",
      "reg_m" -> "0.0"
    )
    
    val sc = createLocalCtx("ExampleApp")
    val ctx = new RequestContext(sc)
    
    val fm = new FM(ctx)
    /**
     * Train polynom (model)
     */
    val (c,v,m) = fm.trainFromFile(args)

    /**
     * Determine error
     */
    val rsme = fm.calculateRMSE(args,c,v,m)
    println("RMSE: " + rsme)
    
    val data = Array(1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0)
    val target = fm.predict(data,c,v,m,10,true,true)
    
    println("Target: " + target)

    
  }
  
  private def createLocalCtx(name:String):SparkContext = {

	System.setProperty("spark.executor.memory", "1g")
		
	val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName(name);
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }

}