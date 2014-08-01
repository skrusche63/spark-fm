package de.kp.spark.fm

import org.apache.spark.{RangePartitioner,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

object FM {

  def train(sc:SparkContext, args:Map[String,String]):(Double,DenseVector,DenseMatrix) = {

    val dataset = args("dataset")
    val num_partitions = args("num_partitions").toInt
    
    /**
     * STEP #1
     * 
     * Partition dataset
     */
    val lines = buildRandomPartitions(sc,dataset,num_partitions)
    
    /**
     * STEP #2
     * 
     * Convert lines partitions and targeted points; a targeted point
     * describes a target value associated with a feature vector
     */
    val database = lines.map(valu => {

      val (partition, line) = valu
      val parts = line.split(',')
      
      val target = parts(0).toDouble
      val features = extractFeatures(parts(1).trim().split(' ').map(_.toDouble))

      (partition,(target,features))
      
    })
    
    /**
     * STEP #3
     * 
     * Calculate model per partition
     */
    val polynom = database.groupByKey().map(valu => {
      
      val algo = new PolySGD(args)
      val num_iter = args("num_iter").toInt
      
      var (c,v,m) = getPolynom(args)
      
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
     * STEP #4
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

  /**
   * Read data from file and use a simple mechanism to partition 
   * input data into a set of almost equal sized datasets
   */  
  def buildRandomPartitions(sc:SparkContext,input:String,num_partitions:Int):RDD[(Int,String)] = {
    
    val file = sc.textFile(input).map(line => {
    
      val ix = new java.util.Random().nextInt(num_partitions)
      (ix,line)
    
    })
    
    file.partitionBy(new RangePartitioner(num_partitions,file))
    
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