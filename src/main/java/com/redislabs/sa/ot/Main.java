package com.redislabs.sa.ot;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import org.apache.commons.lang3.NotImplementedException;
import redis.clients.jedis.*;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * This class is designed to stress-test redis
 * It uses either a JedisCluster client or the Jedis client (your choice as you start it)
 *
 * The default settings result in:
 * 1) Jedis (non-cluster-api version)
 * 2) 3 runs with 50, 100, 150 threads for each successive run
 * to run with default issue:
 * mvn compile exec:java
 *
 * If you pass these arguments: clusterapi true the JedisCluster Stub will be created and used.
 *
 * NB: When deploying a database using a Redis Enterprise distribution this will only work
 * if you configure the target database to support clustering:
 * in the Admin UI for the target database check the box for:
 * 'OSS Cluster API support' enabled
 *
 * mvn compile exec:java -Dexec.args="clusterapi true"
 *
 * You can also specify the number of times the load will be generated
 *
 * mvn compile exec:java -Dexec.args="clusterapi true loop 3"
 *
 * You can also specify the intensity of the load in terms of number of threads
 * by supplying another arg: threadbase 30
 * threadbase will decide how many threads will be spawned during a run for each loop
 * the threadbase will be multiplied by the value of that loop counter
 * in this example the 3 loops will have 200, 400, and 600 threads respectively:
 *
 * mvn compile exec:java -Dexec.args="clusterapi true loop 3 threadbase 200"
 *
 * Note that the order in which you pass the pairs of args does not matter
 * but they must be paired
 *
 * mvn compile exec:java -Dexec.args="threadbase 200 loop 4"
 */
public class Main {

    static final String THREAD_BASE_ARG = "threadbase";
    static final String LOOP_ARG = "loop";
    static final String CLUSTERD_API_ARG = "clusterapi";
    static long exceptionCounter = 0;
    static boolean isClusterAPI = false;
    static int threadBase = 50;
    static int loop = 3;

    public static void main(String[] args){
        long startTime = System.currentTimeMillis();
        if(args.length<1){
            args = new String[]{CLUSTERD_API_ARG,""+isClusterAPI,THREAD_BASE_ARG,""+threadBase,LOOP_ARG,""+loop};
        }
        List<String> argList = Arrays.asList(args);
        Iterator<String> argIterator= argList.iterator();
        System.out.println("starting test with these args: "+argList);
        for (Iterator<String> it = argIterator; it.hasNext(); ) {
            String arg = it.next();
            if(arg.equalsIgnoreCase(CLUSTERD_API_ARG)){
                isClusterAPI = Boolean.parseBoolean(it.next());
                System.out.println(" IN MAIN --> IS CLUSTER API SET TO "+isClusterAPI);
            }
            if(arg.equalsIgnoreCase(THREAD_BASE_ARG)){
                threadBase = Integer.parseInt(it.next());
            }
            if(arg.equalsIgnoreCase(LOOP_ARG)){
                loop = Integer.parseInt(it.next());
            }
        }
        for(int x=1;x<loop;x++) {
            AThreadClass.flushDB();
            loadSortedSetData(threadBase * x,isClusterAPI); // x only grows to be as big as 'loop'
            System.out.println(" IN MAIN --> IS CLUSTER API SET TO "+isClusterAPI);
            weakSearch((threadBase * x),isClusterAPI);//
            try {
                Thread.sleep(3000);
            } catch (Throwable t) {
                t.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("TEST TOOK: " + (endTime - startTime));
            System.out.println("TEST EXPERIENCED: " + exceptionCounter + " Exceptions.");
        }
    }


    public static void weakSearch(int howMany, boolean isClusterAPI){
        new AThreadClass().prepForWeakSearch(howMany,isClusterAPI);
        for(int x=0;x<howMany;x++){
            System.out.println("weakSearch: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass().setIsClusterAPI(isClusterAPI);
            aThreadClass.setTaskName("weakSearch");
            aThreadClass.setTaskNumber(100);
            aThreadClass.setThreadID(x);
            try{
                Thread thread = new Thread(aThreadClass);
                thread.start();
            }catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

    public static void loadSortedSetData(int howMany,boolean isClusterAPI){
        for(int x=0;x<howMany;x++){
            System.out.println("loadSortedSetData: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass();
            aThreadClass.setTaskName("sortedSet");
            aThreadClass.setIsClusterAPI(isClusterAPI);
            aThreadClass.setTaskNumber(1000);
            try{
                // ACRE had no issues with it sleeping or not
                Thread.sleep(200);
                Thread thread = new Thread(aThreadClass);
                thread.start();
            }catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

}

class AThreadClass implements Runnable{
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static JedisCluster jedisCluster = null;

    String taskName;
    int threadID;
    int taskNumber=0;
    boolean orderFront =false;
    boolean isClusterAPI = false;

    public AThreadClass setIsClusterAPI(boolean value){
        isClusterAPI = value;
        if(isClusterAPI){
            jedisCluster = JedisConnectionFactory.getInstance().getJedisCluster();
        }
        return this;
    }

    public void setTaskName(String taskName){
        System.out.println("taskName = "+taskName);
        this.taskName = taskName;
        if(System.nanoTime()%2==0){
            orderFront=true;
        }
    }
    public void setTaskNumber(int taskNumber){
        System.out.println("taskNumber = "+taskNumber);
        this.taskNumber = taskNumber;
    }
    public void setThreadID(int val){
        System.out.println("threadID = "+val);
        this.threadID = val;
    }

    @Override
    public void run() {
        if(taskName.equalsIgnoreCase("sortedSet")) {
            for (int y = 0; y < 10; y++) {
                if(isClusterAPI) {
                    try {
                        for (int x = 0; x < taskNumber / 10; x++) {
                            jedisCluster.zadd("z:voters" + x, y, (100 - (x % 23) + "vote"));
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }else {
                    try(Jedis jedis = jedisPool.getResource()) {
                        for (int x = 0; x < taskNumber / 10; x++) {
                            jedis.zadd("z:voters" + x, y, (100 - (x % 23) + "vote"));
                        }
                    }
                }
            }
        }else if(taskName.equalsIgnoreCase("weakSearch")){
            //try (Jedis jedis = jedisPool.getResource()) {
                //System.out.println(jedis.keys("z:kp:{swcGroupA}:|BQ|L-J*"));
            //}
            String cursor ="0";
            String keyForScan = "";
            if(orderFront==true){
                keyForScan= "z:kp:{swcGroupA}:|ER|L-Xi|Ca*";
            }else{
                keyForScan = "z:kp:{swcGroupA}:|BQ|L-J*";
            }
            if(isClusterAPI) {
                try {
                    ScanResult<String> sr = null;
                    for (int y = 0; y < 10; y++) {
                        ScanParams sp = new ScanParams().match(keyForScan).count(10000);
                        //System.out.println("thread: "+threadID+" cursor == "+cursor+" keyForScan = "+keyForScan);
                        long before = System.currentTimeMillis();
                        sr = jedisCluster.scan("" + cursor, sp);
                        long after = System.currentTimeMillis();
                        if ((after - before) > 4499) {
                            String pattern = "yyyy-MM-dd HH:mm:ssZ";
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                            String date = simpleDateFormat.format(new Date());
                            System.out.println("\n\t\t" + date + "  SCAN... TOOK: " + (after - before));
                        }
                        before = System.currentTimeMillis();
                        String word = "bob";
                        jedisCluster.set("akey{swcGroupA}", word);
                        word = jedisCluster.get("akey{swcGroupA}");
                        StringBuffer buffer = new StringBuffer(word);
                        buffer = buffer.reverse();
                        after = System.currentTimeMillis();
                        if ((after - before) > 4499) {
                            String pattern = "yyyy-MM-dd HH:mm:ssZ";
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                            String date = simpleDateFormat.format(new Date());
                            System.out.println("\n\t\t" + date + "  WRITE AND READ... TOOK: " + (after - before));
                        }
                        cursor = sr.getCursor();
                        //System.out.println(sr.getResult());
                        if (sr.isCompleteIteration()) {
                            break;
                        }
                        if (orderFront == true) {
                            if (sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                                //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                            }
                        } else {
                            if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                                //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 99999 + " Jack --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                            }
                            try {
                                Thread.sleep(10);
                            } catch (Throwable t) { // this is the sleep block only - no need to report on the exception
                            }
                        }
                        if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                            //System.out.println("!!! -Found " + 99999 + " Jack --> Cursor at: " + cursor + " balance is: "+ jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                        } else if (sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                            //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                        }
                    }
                }catch(Throwable t){
                    Main.exceptionCounter++;
                    String pattern = "yyyy-MM-dd HH:mm:ssZ";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    String date = simpleDateFormat.format(new Date());
                    System.out.println(t.getMessage()+"  "+t.getCause()+"\tTRying to get a resource from the pool... " +date); t.printStackTrace();
                }
            }else{ // use jedis not JedisCluster:
                System.out.println("~!!!USING JEDIS NOT JEDIS_CLUSTER...");
                try(Jedis jedis = jedisPool.getResource()){
                    ScanResult<String> sr = null;
                    for (int y = 0; y < 10; y++) {
                        ScanParams sp = new ScanParams().match(keyForScan).count(10000);
                        //System.out.println("thread: "+threadID+" cursor == "+cursor+" keyForScan = "+keyForScan);
                        long before = System.currentTimeMillis();
                        sr = jedis.scan("" + cursor, sp);
                        long after = System.currentTimeMillis();
                        if((after-before)>4499){
                            String pattern = "yyyy-MM-dd HH:mm:ssZ";
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                            String date = simpleDateFormat.format(new Date());
                            System.out.println("\n\t\t"+date+"  SCAN... TOOK: "+(after-before));
                        }
                        before = System.currentTimeMillis();
                        String word = "bob";
                        jedis.set("akey{swcGroupA}",word);
                        word = jedis.get("akey{swcGroupA}");
                        StringBuffer buffer = new StringBuffer(word);
                        buffer = buffer.reverse();
                        after = System.currentTimeMillis();
                        if((after-before)>4499){
                            String pattern = "yyyy-MM-dd HH:mm:ssZ";
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                            String date = simpleDateFormat.format(new Date());
                            System.out.println("\n\t\t"+date+"  WRITE AND READ... TOOK: "+(after-before));
                        }
                        cursor = sr.getCursor();
                        //System.out.println(sr.getResult());
                        if (sr.isCompleteIteration()) {
                            break;
                        }
                        if (orderFront == true) {
                            if (sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                                //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                            }
                        } else {
                            if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                                //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 99999 + " Jack --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                            }
                            try {
                                Thread.sleep(10);
                            } catch (Throwable t) { // this is the sleep block only - no need to report on the exception
                            }
                        }
                        if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                            //System.out.println("!!! -Found " + 99999 + " Jack --> Cursor at: " + cursor + " balance is: "+ jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                        }else if(sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                            //System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                        }
                    }
                }catch(Throwable t){
                    Main.exceptionCounter++;
                    String pattern = "yyyy-MM-dd HH:mm:ssZ";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
                    String date = simpleDateFormat.format(new Date());
                    System.out.println(t.getMessage()+"  "+t.getCause()+"\tTRying to get a resource from the pool... " +date); t.printStackTrace();
                }
            }
        }else{
            throw new NotImplementedException("sorry, no way to do that Hal...");
        }
    }

    public static void flushDB() {
        try  (Jedis jedis = jedisPool.getResource()){
            jedis.flushDB();
            try {
                Thread.sleep(1000);
                System.out.println("Redis Flushed");
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }catch(Throwable t){t.printStackTrace();}
    }

    public void prepForWeakSearch(int howMany, boolean isClusterAPI){
        System.out.println("Loading 100k SortedSets... ");
        int number = howMany;
        if(isClusterAPI) {
            try{
                for (int x = 1; x < 10; x++) {
                    jedisCluster.eval("for index = " + number * x + "," + ((number * x) + number) + ", 1 " +
                                    "do redis.call('ZADD', 'z:kp:{swcGroupA}:|'.. string.char((index%5)+65) .. " +
                                    "string.char(((index+6)%11)+75)..'|' .. (index %2 == 0 and string.char(((index+6)%11)+70).." +
                                    "'-John' or string.char(((index+6)%11)+69)..'-Xi' ) ..'|' .. " +
                                    "(index %3 == 0 and 'Acre' or 'Choi' ) .. '|'.. index ,index,(index%67*2000.97) ) end",
                            1, "swcGroupA");
                    try {
                        Thread.sleep(1000);
                        System.out.println(number * x + " SortedSets written");
                    } catch (Throwable t) {
                    }
                }
                long ax = jedisCluster.zadd("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999",99999,"45000.98");
                System.out.println("added sortedSet with success = "+ax);
                ax = jedisCluster.zadd("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222",22222,"190000.98");
                System.out.println("added sortedSet with success = "+ax);
            }catch(Throwable t){t.printStackTrace();
            }
        }else try (Jedis jedis = jedisPool.getResource()){
            for (int x = 1; x < 10; x++) {
                jedis.eval("for index = " + number * x + "," + ((number * x) + number) + ", 1 " +
                                "do redis.call('ZADD', 'z:kp:{swcGroupA}:|'.. string.char((index%5)+65) .. " +
                                "string.char(((index+6)%11)+75)..'|' .. (index %2 == 0 and string.char(((index+6)%11)+70).." +
                                "'-John' or string.char(((index+6)%11)+69)..'-Xi' ) ..'|' .. " +
                                "(index %3 == 0 and 'Acre' or 'Choi' ) .. '|'.. index ,index,(index%67*2000.97) ) end",
                        1, "swcGroupA");
                try {
                    Thread.sleep(1000);
                    System.out.println(number * x + " SortedSets written");
                } catch (Throwable t) {
                }
            }
            long ax = jedis.zadd("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999",99999,"45000.98");
            System.out.println("added sortedSet with success = "+ax);
            ax = jedis.zadd("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222",22222,"190000.98");
            System.out.println("added sortedSet with success = "+ax);
        }
    }
}

