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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class ElasticFeatureBuilder {

  import de.kp.spark.fm.io.ElasticBuilderFactory._
  
  def createBuilder(mapping:String,names:List[String],types:List[String]):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'
     */
    val builder = XContentFactory.jsonBuilder()
    builder
      .startObject()
        .startObject(mapping)
          .startObject("properties")

            /* timestamp */
            .startObject(TIMESTAMP_FIELD)
              .field("type", "long")
            .endObject()
                    
            /* site */
            .startObject(SITE_FIELD)
              .field("type", "string")
              .field("index", "not_analyzed")
            .endObject()

    (0 until names.length).foreach( i => {
      builder
        .startObject(names(i))
          .field("type",types(i))
        .endObject()

    })

    builder
          .endObject() // properties
        .endObject()   // mapping
      .endObject()
                    
    builder

  }

}