package de.kp.spark.fm

import org.apache.spark.{RangePartitioner,SparkConf,SparkContext}
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

    /**
     * Train polynom (model)
     */
    val (c,v,m) = FM.train(sc,args)

    /**
     * Determine error
     */
    val rsme = FM.calculateRSME(sc,args,c,v,m)
    println(rsme)
    
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