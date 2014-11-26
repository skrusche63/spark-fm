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

import de.kp.spark.core.model._

object Serializer extends BaseSerializer

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)  
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Messages extends BaseMessages {
  
  def FM_BUILDING_STARTED(uid:String):String = String.format("""FM building started for uid '%s'.""", uid)
 
  def DATA_TO_TRACK_RECEIVED(uid:String):String = String.format("""Data to track received for uid '%s'.""", uid)
  
  def MISSING_FEATURES(uid:String):String = String.format("""Features are missing for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)
  
  def MODEL_DOES_NOT_EXIST(uid:String):String = String.format("""Model does not exist for uid '%s'.""", uid)
  
}

object FMStatus extends BaseStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val FINISHED:String = "finished"
  
  val FM:String = "fm"
    
}