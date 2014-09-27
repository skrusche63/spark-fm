package de.kp.spark.fm.redis

import redis.clients.jedis.Jedis
import de.kp.spark.fm.Configuration

object RedisClient {

  def apply():Jedis = {

    val (host,port) = Configuration.redis
    new Jedis(host,port.toInt)
    
  }
  
}