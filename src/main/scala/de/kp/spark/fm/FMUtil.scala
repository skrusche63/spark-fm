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

import java.io.{ObjectInputStream,ObjectOutputStream} 

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem,Path}

private class FMStruct (
  /* Training parameters */
  val init_mean:Double,
  val init_stdev:Double,
      
  val num_attribute:Int,
  val num_factor:Int,
      
  val num_iter:Int,
  val learn_rate:Double,
       
  val k0:Boolean,
  val k1:Boolean,
        
  val reg_c:Double,
  val reg_v:Double,
  val reg_m:Double,
  
  /* Model parameters */
  val c:Double,
  val v:DenseVector,
  val m:DenseMatrix,
  
  /* Metadata */
  val metadata:Seq[(String,Long,Long)]
  
) extends Serializable {}

object FMUtil {
  
  def readMatrix(store:String):DenseMatrix = {

    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val matrix = ois.readObject().asInstanceOf[DenseMatrix]
      
    ois.close()
    
    matrix
    
  }
  
  def readModel(store:String):(Double,DenseVector,DenseMatrix,Map[String,Any],Seq[FMBlock]) = {
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[FMStruct]
      
    ois.close()
    
    /* Training parameters */
    val p = Map(
      "init_mean" -> struct.init_mean,
      "init_stdev" -> struct.init_stdev,
      
      "num_attribute" -> struct.num_attribute,
      "num_factor" -> struct.num_factor,
      
      "num_iter" -> struct.num_iter,
      "learn_rate" -> struct.learn_rate,
      
      "k0" -> struct.k0,
      "k1" -> struct.k1,
      
      "reg_c" -> struct.reg_c,
      "reg_v" -> struct.reg_v,
      "reg_m" -> struct.reg_m
      
    )

    val c = struct.c
    val v = struct.v
    val m = struct.m
    
    val blocks = struct.metadata.map(x => FMBlock(x._1,x._2,x._3))
    
    
    (c,v,m,p,blocks)
    
  }

  def writeMatrix(store:String, matrix:DenseMatrix) {
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(matrix)
    
    oos.close
    
  }
  
  def writeModel(store:String, c:Double, v:DenseVector, m:DenseMatrix, p:Map[String,String], blocks:Seq[FMBlock]) {

    val metadata = blocks.map(block => (block.category,block.begin,block.end))
    
    val struct = new FMStruct(
        
      p("init_mean").toDouble,
      p("init_stdev").toDouble,
      
      p("num_attribute").toInt,
      p("num_factor").toInt,
      
      p("num_iter").toInt,
      p("learn_rate").toDouble,
       
      p("k0").toBoolean,
      p("k1").toBoolean,
        
      p("reg_c").toDouble,
      p("reg_v").toDouble,
      p("reg_m").toDouble,
      
      c,
      v,
      m,
      
      metadata
    )
    
    val conf = new HadoopConf()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
    
  }

}