package de.kp.spark.fm.rest
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

import org.json4s.{DefaultFormats,Formats}
import spray.httpx.Json4sSupport

/**
 * This object ensures that we have json4s support for the 
 * respective marshalling and unmarshalling tasks 
 */
object RestJsonSupport extends Json4sSupport {
  implicit def json4sFormats: Formats = DefaultFormats
}
