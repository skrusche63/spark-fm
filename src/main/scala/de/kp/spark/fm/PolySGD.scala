package de.kp.spark.fm

class PolySGD(args:Map[String,String]) extends Serializable {
    
  val k0 = args("k0").toBoolean
  val k1 = args("k1").toBoolean

  /**
   * Extract regularization parameters
   */
  val reg_c = args("reg_c").toDouble
  val reg_v = args("reg_v").toDouble
  val reg_m = args("reg_m").toDouble

  val learn_rate = args("learn_rate").toDouble
  val num_factor = args("num_factor").toInt

  def gradient(target:Double,features:SparseVector,c:Double,v:DenseVector,m:DenseMatrix):(Double,DenseVector,DenseMatrix) = {
	  	  
	val card: Int = features.size  	   
	val sum = Array.fill(num_factor)(0.0)
	  
	/** 
	 * This method computes the target value (y_i) for a certain
	 * row from the set of actual model parameters; these parameters
	 * vary due to iteration and learning (training)
	 */
	val y_p: Double = predict(features,sum,c,v,m)

	/** 
	 * The parameter 'multiplier' is associated with the gradient of the
	 * loss function (y_e - y_p)*(y_e - y_p); see S. Rendle:
	 * 
	 * Calculate difference between existing target value and predicted 
	 * target value
	 */
		
  	/**
  	 * See loss function in S. Rendle, eq. (11) 
  	 */
	val multiplier: Double = y_p - target
	
	def calculateGradient(multiplier:Double, sum:Array[Double], c:Double, v:DenseVector, m:DenseMatrix):(Double,DenseVector,DenseMatrix) = {
      /*
       * Calculate gradient for constant part of the model
       */
	  val grad_c = (if (k0) {- learn_rate * (multiplier + reg_c * c)} else 0.0)	    
	  /*
	   * Calculate gradient for the linear (vector) part of the model; we use a
	   * for loop here as this is faster than DenseVector operations, which loops 
	   * multiple times of the vector elements
	   */
	  val grad_v = DenseVector.zeros(card)  
	  if (k1) {
		for (i <- 0 until card) {          
		  grad_v.update(i, - learn_rate * (multiplier * features(i) + reg_v * v(i)))
		}		
	  }	    
	  /*
	   * Calculate gradient for the quadratic (matrix) part of the model; we use 
	   * for loops here as this is faster than DenseMatrix operations
	   */
	  val grad_m = DenseMatrix.zeros(num_factor,card)
	  for (f <- 0 until num_factor) {			
		for (i <- 0 until card) {

		  val x_i = features(i)
		    
		  val m_fi = m(f,i)
		  val grad = sum(f) * x_i - m_fi * x_i * x_i

		  grad_m.update(f,i, - learn_rate * (multiplier * grad + reg_v * m_fi))

		}		  
	  }	  
	  
	  (grad_c,grad_v,grad_m)

	}
	
	calculateGradient(multiplier,sum,c,v,m)
	
  }

  private def predict(features:SparseVector,sum:Array[Double],c:Double,v:DenseVector,m:DenseMatrix): Double = {
		
	val card: Int = features.size
				
	var result: Double = 0.0
		
	/* 
	 * Add constant value c to target value
	 */
	if (k0) result += c			
    /* 
     * Add linear part of the FM equation
     */
	if (k1) {
	  for (i <- 0 until card) {
		result += v(i) * features(i)
	  }			
	}
	/* 
	 * Add quadratic part of the FM equation; 
	 * num_factor is the number of factorization 
	 * parameters
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

}