package de.kp.spark.fm.model
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

import de.kp.spark.core.model._

case class ScoredColumn(col:Int,score:Double)
case class SimilarColumns(col:Int,items:List[ScoredColumn])

case class Similars(items:List[SimilarColumns])

object Serializer extends BaseSerializer {
  
  def serializeSimilars(similars:Similars):String = write(similars) 
  def deserializeSimilars(similars:String):Similars = read[Similars](similars)
  
}

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PARQUET:String = "PARQUET"    
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PARQUET,PIWIK)  
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Messages extends BaseMessages {
   
  def MATRIX_TRAINING_STARTED(uid:String):String = 
    String.format("""[UID: %s] Matrix training started.""", uid)
 
  def MODEL_TRAINING_STARTED(uid:String):String = 
    String.format("""[UID: %s] Model training started.""", uid)
   
  def MATRIX_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] Matrix does not exist.""", uid)
 
  def MISSING_FEATURES(uid:String):String = String.format("""Features are missing for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)
  
  def MODEL_DOES_NOT_EXIST(uid:String):String = 
    String.format("""[UID: %s] Model does not exist.""", uid)
  
}

object FMStatus extends BaseStatus