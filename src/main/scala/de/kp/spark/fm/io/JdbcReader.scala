package de.kp.spark.fm.io
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

import java.sql.{Connection,DriverManager,ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD,RDD}

import de.kp.spark.fm.Configuration
import scala.collection.mutable.HashMap

class JdbcReader(@transient sc:SparkContext,site:Int,query:String) {

  protected val MYSQL_DRIVER   = "com.mysql.jdbc.Driver"
  protected val NUM_PARTITIONS = 1
   
  protected val (url,database,user,password) = Configuration.mysql
 
  def read(fields:List[String] = List.empty[String]):RDD[Map[String,Any]] = {
    /*
     * The value of 'site' is used as upper and lower bound for 
     * the range (key) variable of the database table
     */
    val result = new JdbcRDD(sc,() => getConnection(url,database,user,password),
      query,site,site,NUM_PARTITIONS,
      (rs:ResultSet) => getRow(rs,fields)
    ).cache()

    result
    
  }
  
  /**
   * Convert database row into Map[String,Any] and restrict
   * to column names that are defined by the field spec
   */
  protected def getRow(rs:ResultSet,fields:List[String]):Map[String,Any] = {
    val metadata = rs.getMetaData()
    val numCols  = metadata.getColumnCount()
    
    val row = HashMap.empty[String,Any]
    (1 to numCols).foreach(i => {
      
      val k = metadata.getColumnName(i)
      val v = rs.getObject(i)
      
      if (fields.isEmpty) {
        row += k -> v
        
      } else {        
        if (fields.contains(k)) row += k -> v
        
      }
      
    })

    row.toMap
    
  }
  
  protected def getConnection(url:String,database:String,user:String,password:String):Connection = {

    /* Create MySQL connection */
	Class.forName(MYSQL_DRIVER).newInstance()	
	val endpoint = getEndpoint(url,database)
		
	/* Generate database connection */
	val	connection = DriverManager.getConnection(endpoint,user,password)
    connection
    
  }
  
  protected def getEndpoint(url:String,database:String):String = {
		
	val endpoint = "jdbc:mysql://" + url + "/" + database
	endpoint
		
  }

}