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
import scala.concurrent.Future
import scala.collection.mutable.HashMap

object RemoteContext {

  val clientPool = HashMap.empty[String,RemoteClient]
 
  /**
   * Send message to remote Akka service
   */
  def send(service:String,message:String):Future[Any] = {
   
    if (clientPool.contains(service) == false) {
      clientPool += service -> new RemoteClient(service)      
    }
   
    val client = clientPool(service)
    client.send(message)
 
  }

  /**
   * Inform all registered listener about a certain situation
   * and send provided message
   */
  def notify(message:String) {
    
    val listeners = Listeners.names()
    
    for (listener <- listeners) {
      send(listener,message)
    }
    
  }

}