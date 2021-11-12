# RedisHackingTesting
A  place to experiment with Redis in Java

Test one:  Using the Redis Enterprise Proxy with OSS cluster API support turned off:
1) make sure the jedisconnectionfactory.properties file has the correct host and port and pool configuration settings you desire.
2) Configure your redis Enterprise Clustered database with OSS CLuster Support NOT enabled
3) execute this:
mvn compile exec:java -Dexec.args="loop 2 threadbase 300"

This often results in:
redis.clients.jedis.exceptions.JedisConnectionException: java.net.SocketTimeoutException: Read timed out
        at redis.clients.jedis.util.RedisInputStream.ensureFill(RedisInputStream.java:205)
        at redis.clients.jedis.util.RedisInputStream.readByte(RedisInputStream.java:43)
        at redis.clients.jedis.Protocol.process(Protocol.java:155)
        at redis.clients.jedis.Protocol.read(Protocol.java:220)
        at redis.clients.jedis.Connection.readProtocolWithCheckingBroken(Connection.java:283)
        at redis.clients.jedis.Connection.getUnflushedObjectMultiBulkReply(Connection.java:245)
        at redis.clients.jedis.Connection.getObjectMultiBulkReply(Connection.java:250)
        at redis.clients.jedis.Jedis.scan(Jedis.java:3360)
        at com.redislabs.sa.ot.AThreadClass.run(Main.java:264)
        at java.base/java.lang.Thread.run(Thread.java:833)
Caused by: java.net.SocketTimeoutException: Read timed out
        at java.base/sun.nio.ch.NioSocketImpl.timedRead(NioSocketImpl.java:283)
        at java.base/sun.nio.ch.NioSocketImpl.implRead(NioSocketImpl.java:309)
        at java.base/sun.nio.ch.NioSocketImpl.read(NioSocketImpl.java:350)
        at java.base/sun.nio.ch.NioSocketImpl$1.read(NioSocketImpl.java:803)
        at java.base/java.net.Socket$SocketInputStream.read(Socket.java:966)
        at java.base/java.io.InputStream.read(InputStream.java:218)
        at redis.clients.jedis.util.RedisInputStream.ensureFill(RedisInputStream.java:199)


Test two:  Using the Redis Enterprise Proxy with OSS cluster API support turned on:
1) make sure the jedisconnectionfactory.properties file has the correct host and port and pool configuration settings you desire.
2) Configure your redis Enterprise Clustered database with OSS CLuster Support enabled ( turned ON ) 
3) execute this:
 mvn compile exec:java -Dexec.args="clusterapi true loop 2 threadbase 300"

This should not result in any TimeoutExceptions.

