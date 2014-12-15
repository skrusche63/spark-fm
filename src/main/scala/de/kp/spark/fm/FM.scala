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

import org.apache.spark.{RangePartitioner,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import de.kp.spark.core.source.FileSource
import de.kp.spark.fm.source.{FeatureModel}

object FM extends Serializable {

  def trainFromFile(@transient sc:SparkContext,params:Map[String,String]):(Double,DenseVector,DenseMatrix) = {
    
    val model = new FeatureModel(sc)
    val partitions = params("num_partitions").toInt
    
    val path = Configuration.input(0)
    
    val rawset = new FileSource(sc).connect(path,null)
    val dataset = model.buildFile(null,rawset,partitions)
    
    trainFromRDD(dataset,params)
    
  }
  
  def trainFromRDD(dataset:RDD[(Int,(Double,SparseVector))],params:Map[String,String]):(Double,DenseVector,DenseMatrix) = {
    
    val num_partitions = params("num_partitions").toInt
        
    /**
     * STEP #1
     * 
     * Calculate model per partition
     */
    val polynom = dataset.groupByKey().map(valu => {
      
      val algo = new PolySGD(params)
      val num_iter = params("num_iter").toInt
      
      var (c,v,m) = getPolynom(params)
      
      val (partition,data) = valu      
      for (iter <- 0 until 20) {
        for ((target,features) <- data) {
          /**
           * Calculate gradient for polynom
           */
          val (g_c,g_v,g_m) = algo.gradient(target,features,c, v, m)
          /**
           * Update polynom
           */
          c = c + g_c
          v = v + g_v
          m = m + g_m
        
        }      
      }
      
      (partition,c,v,m)
      
    })
    
    /**
     * STEP #2
     * 
     * Build mean values
     */    
    val raw = polynom.collect()
    
    val scale = (1.0 / num_partitions)
    
    val mean_c = raw.map(valu => valu._2).reduceLeft(_ + _) * scale
    val mean_v = raw.map(valu => valu._3).reduceLeft(_ + _) * scale
    val mean_m = raw.map(valu => valu._4).reduceLeft(_ + _) * scale
    
    (mean_c, mean_v, mean_m)
    
  }
  
  def calculateRSME(sc:SparkContext,args:Map[String,String],c:Double,v:DenseVector,m:DenseMatrix):Double = {
    
    val dataset = args("dataset")
    
    val k0 = args("k0").toBoolean
    val k1 = args("k1").toBoolean

    val num_factor = args("num_factor").toInt
    val file = sc.textFile(dataset)
    
    val rsme = file.map(valu => {

      val parts = valu.split(',')
      
      val target = parts(0).toDouble
      val features = extractFeatures(parts(1).trim().split(' ').map(_.toDouble))

	  val y_p = predict(features,c,v,m,num_factor,k0,k1)

	  val err: Double = y_p - target
	  err*err
      
    })

    Math.sqrt(rsme.collect().sum / file.count())

  }
  
  def predict(data:Array[Double],c:Double,v:DenseVector,m:DenseMatrix,num_factor:Int,k0:Boolean,k1:Boolean): Double = {
  
    val features = extractFeatures(data)
    predict(features,c,v,m,num_factor,k0,k1)
  
  }

  private def predict(features:SparseVector,c:Double,v:DenseVector,m:DenseMatrix,num_factor:Int,k0:Boolean,k1:Boolean): Double = {

    val sum = Array.fill(num_factor)(0.0)

	val card: Int = features.size
				
	/* 
	 * The target value to calculate
	 */
	var result: Double = 0.0
		
	/* 
	 * Add constant value c to target value
	 */
	if (k0) {
	  result += c		
	}
    /* 
     * Add linear part of the FM equation
     */
	if (k1) {
	  for (i <- 0 until card) {
		result += v(i) * features(i)
	  }
			
	}
	/* Add quadratic part of the FM equation
	 */
	val sum_sqr = Array.fill(num_factor)(0.0)
	for (f <- 0 until num_factor) {

	  // initialize computation parameters; for more detail, see S. Rendle, Equation (5)
	  // http://www.csie.ntu.edu.tw/~b97053/paper/Factorization%20Machines%20with%20libFM.pdf
	  for (i <- 0 until card) {
			  
		val d = m(f,i) * features(i)
				
		sum.update(f, sum(f) + d)				
		sum_sqr.update(f, sum_sqr(f) + d*d)
				
	  }
			
	  result += 0.5 * (sum(f)*sum(f) - sum_sqr(f))
	
	}

	result
	
  }

  /**
   * Helper method to build the initial SGD model
   */
  private def getPolynom(settings:Map[String,String]):(Double,DenseVector,DenseMatrix) = {
    
    /**
     * Factors to compute the initial fill of the matrix (v)
     */
    val init_mean:Double  = settings("init_mean").toDouble
    val init_stdev:Double = settings("init_stdev").toDouble
    
    val num_attribute:Int = settings("num_attribute").toInt
    val num_factor:Int    = settings("num_factor").toInt
    
    val c:Double = 0.0    
    val v = DenseVector.zeros(num_attribute)  
    
    val rand = SparkRandom.randomGaussian(init_mean, init_stdev)    
    val m = DenseMatrix.random(num_factor, num_attribute, rand)
        
    (c, v, m)

  }
  
  /**
   * This is a helper method to build a sparse vector from the input data;
   * to this end, we reduce to such entries that are different from zero
   */
  private def extractFeatures(raw:Array[Double]):SparseVector = {
    
    val vector = new SparseVector(raw.length)
    
    for (i <- 0 until raw.length) {
    	
      val array_i: Double = raw(i)
      if (array_i > 0) vector.update(i, array_i)
        
    }
    
    vector
  
  }

}