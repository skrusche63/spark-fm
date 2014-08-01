package de.kp.spark.fm

import scala.util.Random
import scala.util.control.Breaks._

object SparkRandom {

  def randomGamma(alpha: Double): Double = {
	  
	if (alpha < 1.0) {

	  var u: Double = Double.NaN

	  do {
		u = randomUniform()
	  } while (u == 0.0);

	  return randomGamma(alpha + 1.0) * math.pow(u, 1.0 / alpha)

	} else {

      // Marsaglia and Tsang: A Simple Method for Generating Gamma Variables
	  var d,c,x,v,u: Double = Double.NaN

		d = alpha - 1.0/3.0
		c = 1.0 / math.sqrt(9.0 * d)

		do {
		  do {
			x = randomGaussian()
			v = 1.0 + c*x

		  } while (v <= 0.0)

			v = v * v * v
			u = randomUniform()

		} while ( 
		  (u >= (1.0 - 0.0331 * (x*x) * (x*x))) && (math.log(u) >= (0.5 * x * x + d * (1.0 - v + math.log(v))))
		  )

		return d*v

	}
  }

  def randomGamma(alpha: Double, beta: Double): Double = {
	randomGamma(alpha) / beta
  }
	
  def randomGaussian(mean: Double, stdev: Double): Double = {

	if ((stdev == 0.0) || (stdev.isNaN())) {
	  mean

	} else {
	  mean + stdev*randomGaussian()
	}

  }

  def randomGaussian(): Double = {
	  
	// Joseph L. Leva: A fast normal Random number generator
	var u, v, x, y, Q: Double = Double.NaN

	breakable {
	  do {
		do {
		  u = randomUniform()				
		} while (u == 0.0) 
	
		v = 1.7156 * (randomUniform() - 0.5)
		x = u - 0.449871
	
		y = Math.abs(v) + 0.386595
		Q = x*x + y*(0.19600*y-0.25472*x)
				
		if (Q < 0.27597) break 
		
	
	  } while ((Q > 0.27846) || ( (v*v) > (-4.0 * u * u * Math.log(u)) ))
	}
		
	return v / u
		
  }

  def randomUniform(): Double = {
		
	val rand: Random = new Random()
	rand.nextDouble()
		
  }
  
}