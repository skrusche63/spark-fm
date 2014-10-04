package de.kp.spark.fm.hadoop
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import org.apache.hadoop.conf.{Configuration => HadoopConf}

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.io.{SequenceFile,Text}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import de.kp.spark.fm.{DenseVector,DenseMatrix}

private case class Params(num_factor:Int,k0:Boolean,k1:Boolean)

object HadoopIO {

  def writeFM(c:Double,v:DenseVector,m:DenseMatrix,p:Map[String,Any],path:String) {
    
    /* Write params */
    writeToHadoop(serializeParams(p), path + "/params")

    /* Write c */    
    writeToHadoop(c.toString, path + "/c")

    /* Write v */    
    writeToHadoop(serializeDV(v), path + "/v")
    
    /* Write m */
    writeToHadoop(serializeDM(m), path + "/m")
    
  }
  
  def readFM(path:String):(Double,DenseVector,DenseMatrix,Map[String,Any]) = {
   
    /* Read params */
    val p = deserializeParams(readFromHadoop(path + "/params"))

    /* Read c */
    val c = readFromHadoop(path + "/c").toDouble

    /* Read v */
    val v = deserializeDV(readFromHadoop(path + "/v"))

    /*
     * Read m
     */
    val m = deserializeDM(readFromHadoop(path + "/m"))
    
    (c,v,m,p)
    
  }
  
  private def deserializeParams(p:String):Map[String,Any] = {

    implicit val formats = Serialization.formats(NoTypeHints)
	val params = read[Params](p)
    
	Map("num_factor" -> params.num_factor, "k0" -> params.k0, "k1" -> params.k1)
	
  }

  private def serializeParams(params:Map[String,Any]):String = {

    implicit val formats = Serialization.formats(NoTypeHints)
    
    val k0 = params("k0").asInstanceOf[Boolean]
    val k1 = params("k1").asInstanceOf[Boolean]

    val num_factor = params("num_factor").asInstanceOf[Int]
	write(new Params(num_factor,k0,k1))
    
  }

  private def serializeDM(matrix:DenseMatrix):String = {
    
    val values = ArrayBuffer.empty[String]
    
    val rows = matrix.getRowDim
    for (i <- 0 until rows) {
      
      val vector = matrix.getRow(i)
      val row = ArrayBuffer.empty[Double]

      for (element <- vector) {
        row += element      
      }
      
      values += row.mkString(",")
    
    }

    values.mkString(";")
    
  }

  private def deserializeDM(matrix:String):DenseMatrix = {
    
    val rows = matrix.split(";")    
    
    val rdim = rows.length;    
    val cdim = rows(0).split(",").length
    
    val values = Array.fill[Double](rdim,cdim)(0)
    
    (0 until rdim).foreach(i => {
      values(i) = rows(i).split(",").map(_.toDouble)
    })
    
    new DenseMatrix(values)
    
  }

  private def serializeDV(vector:DenseVector):String = {
    
    val values = ArrayBuffer.empty[Double]

    val iterator= vector.get()
    for (element <- iterator) {       
      values += element      
    }
    
    values.mkString(",")
    
  }

  private def deserializeDV(vector:String):DenseVector = {
    
    val values = vector.split(",").map(_.toDouble)
    new DenseVector(values)
    
  }
  
  private def writeToHadoop(ser:String,file:String) {

    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
	  val writer = new SequenceFile.Writer(fs, conf, path, classOf[Text], classOf[Text])

	  val k = new Text()
	  val v = new Text(ser)

	  writer.append(k,v)
	  writer.close()

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}
 
  }
  
  private def readFromHadoop(file:String):String = {
    
    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
      
      val reader = new SequenceFile.Reader(fs,path,conf)

      val k = new Text()
      val v = new Text()

      reader.next(k, v)
      reader.close()
      
      v.toString

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}

  }
  
}