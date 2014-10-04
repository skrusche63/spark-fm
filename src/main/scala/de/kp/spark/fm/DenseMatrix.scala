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
  
  def getRow(index:Int):Array[Double] = data(index)
  
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