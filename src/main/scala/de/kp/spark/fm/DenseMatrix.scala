package de.kp.spark.fm

object DenseMatrix {
  
  def apply(elements: Array[Array[Double]]) = new DenseMatrix(elements)

  def apply(rdim:Int, cdim:Int) = new DenseMatrix(rdim, cdim)
  
  def random(rdim: Int, cdim: Int, rand: Double): DenseMatrix = {
 
    val matrix = new DenseMatrix(rdim, cdim)
    for (i <- 0 until rdim) {
      for (j <- 0 until cdim) {
        matrix.update(i, j, rand)
      }
    }
    
    matrix

  }
  
  def zeros(rdim: Int, cdim: Int) = new DenseMatrix(rdim, cdim)

  //def ones(length: Int) = Vector(length, _ => 1)

//  /**
//   * Creates this [[org.apache.spark.util.Vector]] of given length containing random numbers 
//   * between 0.0 and 1.0. Optional scala.util.Random number generator can be provided.
//   */
//  def random(length: Int, random: Random = new XORShiftRandom()) =
//    Vector(length, _ => random.nextDouble())
//
//  class Multiplier(num: Double) {
//    def * (vec: Vector) = vec * num
//  }

//  implicit def doubleToMultiplier(num: Double) = new Multiplier(num)

//  implicit object MatrixAccumParam extends org.apache.spark.AccumulatorParam[FMMatrix] {
//    def addInPlace(t1: FMMatrix, t2: FMMatrix) = t1 + t2
//
//    def zero(initialValue: FMMatrix) = FMMatrix.zeros(initialValue.rdim, initialValue.cdim)
//    
//  }

}

class DenseMatrix extends Serializable {

  var rdim:Int = 0
  var cdim:Int = 0
  
  private var data:Array[Array[Double]] = null
  
  def this(dim1:Int, dim2:Int) {
    this()
    
    rdim = dim1
    cdim = dim2
    
    data = Array.fill(rdim,cdim)(0.0)
    
  }
  
  def apply(i: Int, j:Int) = data(i)(j)

  def this(elements: Array[Array[Double]]) {
    this()
    
    rdim = elements.length
    cdim = elements(0).length
    
    data = elements
    
  }
  
  def + (other: DenseMatrix): DenseMatrix = {
    
    if ((rdim != other.rdim) || (cdim != other.cdim)) {
      throw new IllegalArgumentException("Matrices of different size")
    }
    
    for (i <- 0 until rdim) yield {
      for (j <- 0 until cdim) yield {
        DenseMatrix.this.update(i, j, DenseMatrix.this(i, j) + other(i,j))        
      }
    }
    
    DenseMatrix.this
    
  }

  def getRowDim() = rdim
  
  def getColDim() = cdim
  
  def get() = data
  
  def add(other: DenseMatrix) = DenseMatrix.this + other

  def - (other: DenseMatrix): DenseMatrix = {

    if ((rdim != other.rdim) || (cdim != other.cdim)) {
      throw new IllegalArgumentException("Matrices of different size")
    }
    
    for (i <- 0 until rdim) yield {
      for (j <- 0 until cdim) yield {
        DenseMatrix.this.update(i, j, DenseMatrix.this(i, j) - other(i,j))        
      }
    }
    
    DenseMatrix.this
  
  }

  def subtract(other: DenseMatrix) = DenseMatrix.this - other

//  def dot(other: Vector): Double = {
//    if (length != other.length) {
//      throw new IllegalArgumentException("Vectors of different length")
//    }
//    var ans = 0.0
//    var i = 0
//    while (i < length) {
//      ans += this(i) * other(i)
//      i += 1
//    }
//    ans
//  }

  def += (other: DenseMatrix): DenseMatrix = {

    if ((rdim != other.rdim) || (cdim != other.cdim)) {
      throw new IllegalArgumentException("Matrices of different size")
    }
   
    for (i <- 0 until rdim) yield {
      for (j <- 0 until cdim) yield {
        data(i).update(j, DenseMatrix.this(i, j) + other(i,j))        
      }
    }
    
    DenseMatrix.this

  }
  def * (scale: Double): DenseMatrix = {

    for (i <- 0 until rdim) {
      for (j <- 0 until cdim) {
        data(i).update(j, data(i)(j)*scale)
      }
    }
    
    DenseMatrix.this
    
  }
  
  def update(i:Int, j:Int, value:Double) {
    data(i).update(j, value)
  }
  
  def addInPlace(other: DenseMatrix) = DenseMatrix.this += other
  
}