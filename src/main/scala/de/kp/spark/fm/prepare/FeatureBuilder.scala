package de.kp.spark.fm.prepare
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

import org.apache.spark.rdd.RDD

/**
 * TODO: This class is under construction 
 */
object FeatureBuilder extends Serializable {
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds

  /**
   * This method transforms a tuple of (user,item,rating,timestamp,event) into
   * a feature vector that can be used to train a factorization model.
   * 
   * The feature vector has the following architecture:
   * 
   * a) user block describing the active user
   * 
   * b) item block describing the active item
   * 
   * Every information in addition to the active user & item is considered
   * as context that has some influence onto the user-item rating
   * 
   * c) items rated block (value = 1/N where N is the number of items rated so far)
   *         
   * Hypothesis: The items rated so far by the active user do influence the rating
   * of the active item; this requires to have access onto historical rating data
   * of the active user        
   * 
   * d) last items rated before active item
   * 
   * Hypothesis: The items rated before the active item has some influence onto
   * the rating of the active item; this also requires to have access onto some
   * historical data of the active user
   *         
   * e) datetime block of user-item-rating (should be a single column)
   * 
   * Hypothesis: The datetime of the respective user-item-rating influences the
   * rating of the active items
   *         
   * f) event block of user-item-rating (should be a single column)
   * 
   */
  def buildFeatures(ratings:RDD[(String,String,String,Int,Long,Int)]) {
    
    val sc = ratings.context
    
    // TODO: Note, that these data structure are rebuilt from Redis
    val edict = new Dict()
    val idict = new Dict()
    val udict = new Dict()
    /* 
     * Build broadcast variables from externally provided
     * data dictionaries
     */
    val b_edict = sc.broadcast(edict)
    val b_idict = sc.broadcast(idict)
    val b_udict = sc.broadcast(udict)
    /*
     * Group all ratings by site,user; note, that it is recommended
     * to build the features for a single 'site'
     */
    val userRatings = ratings.groupBy(x => (x._1,x._2))
    val features = userRatings.map(x => {
      
      val (site,user) = x._1
      /*
       * Determine user block; we actually ignore the 'site'
       * parameter here, as it is expected that the respective
       * directories are retrieved on a per 'site' basis
       * 
       * The 'user' block is the same for all user-specific
       * feature vectors
       */
      val ublock = activeUserBlock(user,b_udict.value)
      val data = x._2.map(v => (v._3,v._4,v._5,v._6))
      /*
       * Retrieve items rated by the user and turn into 
       * respective 'items rated block'; this block is
       * the same for all user-specific feature vectors
       */
      val iids = data.map(_._1).toList
      val oblock = ratedItemsBlock(iids,b_idict.value)
      
      /*
       * Sort items and build data structure to determine
       * the item rated before the active item
       */
      val latest = data.map(x => (x._1,x._3)).toList.sortBy(_._2)
      
      val targetedPoints = data.map(v => {
        
        val (item,rating,timestamp,event) = v
        
        /* Build block to descibe the active (rated) item */
        val iblock = activeItemBlock(item,b_idict.value)
        
        /* Build event block */
        val eblock = eventBlock(event.toString,b_edict.value)
        /*
         * Build time block: we do not use the exact timestamp
         * here as ratings within the same day are considered
         * to be equal 
         */
        val day = (timestamp / DAY).toDouble
        val tblock = Array.fill[Double](1)(day)
        /* 
         * Determine the item that has been rated just 
         * before the active item
         */
        val pos = latest.indexOf((item,timestamp))
        val before = if (pos > 0) latest(pos-1)._1 else null
        
        val bblock = beforeItemBlock(before,b_idict.value)
        
        /* Build feature vector */
        val features = ublock ++ iblock ++ oblock ++ tblock ++ eblock ++ bblock
        
        /* Build target variable */
        val target = rating.toDouble
        
        (target,features)
        
      })
      
    })
    
  }
  
  /**
   * This method determines the user vector part from the externally
   * known and provided unique user identifier
   */
  private def activeUserBlock(uid:String,udict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](udict.size)(0.0)
    
    val pos = udict.getLookup(uid)
    block(pos) = 1
    
    block
    
  }

  /**
   * This method determines the item vector part from the externally
   * known and provided unique item identifier
   */
  private def activeItemBlock(iid:String, idict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](idict.size)(0.0)
    
    val pos = idict.getLookup(iid)
    block(pos) = 1
    
    block
    
  }
  private def beforeItemBlock(iid:String, idict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](idict.size)(0.0)
    if (iid == null) return block
    
    val pos = idict.getLookup(iid)
    block(pos) = 1
    
    block
    
  }
  
  private def ratedItemsBlock(iids:List[String], idict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](idict.size)(0.0)
    val weight = 1.toDouble / iids.size
    
    for (iid <- iids) {
      val pos = idict.getLookup(iid)
      block(pos) = weight      
    }
    
    block
    
  }
  
   /**
   * This method determines the event vector part from the externally
   * known and provided unique event identifier
   */
  private def eventBlock(eid:String, edict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](edict.size)(0.0)
    
    val pos = edict.getLookup(eid)
    block(pos) = 1
    
    block
    
  }
 
  def main(args:Array[String]) {
    
    val u = Array(1,2,3)
    val v = Array(4,5,6)
    
    val w = u ++ v
    
  }
}