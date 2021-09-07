package com.redislabs.sa.ot;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import io.rebloom.client.Client;
import org.apache.commons.lang3.NotImplementedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class Main {


    public static void main(String[] args){
        System.out.println("testing");
        for(int x=0;x<6;x++) {
            AThreadClass.flushDB();
            loadSortedSetData((20*x)+20);
            weakSearch((100*x)+200);
            System.out.println("All Threads kicked off...");
        }
    }

    public static void weakSearch(int howMany){
        AThreadClass.prepForWeakSearch(howMany);
        for(int x=0;x<howMany;x++){
            System.out.println("weakSearch: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass();
            aThreadClass.setTaskName("weakSearch");
            aThreadClass.setTaskNumber(100);
            aThreadClass.setThreadID(x);
            try{
                // ACRE had no issues with it sleeping or not
                //Thread.sleep(200);
                Thread thread = new Thread(aThreadClass);
                thread.start();
            }catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

    public static void loadSortedSetData(int howMany){
        for(int x=0;x<howMany;x++){
            System.out.println("loadSortedSetData: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass();
            aThreadClass.setTaskName("sortedSet");
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

    String taskName;
    int threadID;
    int taskNumber=0;
    boolean orderFront =false;

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
                try (Jedis jedis = jedisPool.getResource()) {
                    for (int x = 0; x < taskNumber / 10; x++) {
                        jedis.zadd("z:voters" + x, y, (100 - (x % 23) + "vote"));
                    }
                }
            }
        }else if(taskName.equalsIgnoreCase("weakSearch")){
            try (Jedis jedis = jedisPool.getResource()) {
                System.out.println(jedis.keys("z:kp:{swcGroupA}:|BQ|L-J*"));
            }
            String cursor ="0";
            String keyForScan = "";
            if(orderFront==true){
                keyForScan= "z:kp:{swcGroupA}:|ER|L-Xi|Ca*";
            }else{
                keyForScan = "z:kp:{swcGroupA}:|BQ|L-J*";
            }
            try (Jedis jedis = jedisPool.getResource()) {
                ScanResult<String> sr = null;
                for (int y = 0; y < 10; y++) {
                    ScanParams sp = new ScanParams().match(keyForScan).count(10000);
                    //System.out.println("thread: "+threadID+" cursor == "+cursor+" keyForScan = "+keyForScan);
                    sr = jedis.scan("" + cursor, sp);
                    cursor = sr.getCursor();
                    //System.out.println(sr.getResult());
                    if (sr.isCompleteIteration()) {
                        break;
                    }
                    if (orderFront == true) {
                        if (sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                            System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                        }
                    } else {
                        if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                            System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 99999 + " Jack --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                        }
                        try {
                            Thread.sleep(10);
                        } catch (Throwable t) {
                        }
                    }
                    if (sr.getResult().contains("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999")) {
                        System.out.println("!!! -Found " + 99999 + " Jack --> Cursor at: " + cursor + " balance is: "+ jedis.zrangeByScore("z:kp:{swcGroupA}:|BQ|L-Jack|Masters|99999", 99999, 99999));
                    }else if(sr.getResult().contains("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222")) {
                        System.out.println("!!! ThreadID: " + threadID + " Cursor at: " + cursor + " -Found " + 22222 + " Catapult --> " + jedis.zrangeByScore("z:kp:{swcGroupA}:|ER|L-Xi|Catapult|22222", 22222, 22222));
                    }
                }
            }
        }else{
            throw new NotImplementedException("sorry, no way to do that Hal...");
        }
    }

    public static void flushDB() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushDB();
            try {
                Thread.sleep(1000);
                System.out.println("Redis Flushed");
            } catch (Throwable t) {
            }
        }
    }

    public static void prepForWeakSearch(int howMany){
        System.out.println("Loading 100k SortedSets... ");
        int number = 10000;
        try (Jedis jedis = jedisPool.getResource()) {
            for (int x = 1; x < 10; x++) {
                jedis.eval("for index = " + number * x + "," + ((number * x) + 10000) + ", 1 " +
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