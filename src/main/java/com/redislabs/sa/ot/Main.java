package com.redislabs.sa.ot;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import io.rebloom.client.Client;
import org.apache.commons.lang3.NotImplementedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class Main {

    //static JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();


    public static void main(String[] args){
        System.out.println("testing");
        loadSortedSetData(400);
        System.out.println("testing2");
        //loadTopKData(1000);
        //loadTSData(1000);
    }

    public static void loadSortedSetData(int howMany){
        for(int x=0;x<howMany;x++){
            System.out.println("loadSortedSetData: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass();
            aThreadClass.setTaskName("sortedSet");
            aThreadClass.setTaskNumber(1000);
            try{
                // ACRE had no issues with it sleeping or not
                // RE Container broke either way
                Thread.sleep(200);
                Thread thread = new Thread(aThreadClass);
                thread.start();
            }catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

    public static void loadTopKData(int howMany){
        for(int x=1;x==howMany;x++){
            System.out.println("loadTopKData: Starting Thread..."+x);
            AThreadClass aThreadClass = new AThreadClass();
            aThreadClass.setTaskName("topK");
            aThreadClass.setTaskNumber(5000);
            try{
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
    int taskNumber=0;

    public void setTaskName(String taskName){
        System.out.println("taskName = "+taskName);
        this.taskName = taskName;
    }
    public void setTaskNumber(int taskNumber){
        System.out.println("taskNumber = "+taskNumber);
        this.taskNumber = taskNumber;
    }

    @Override
    public void run() {
        if(taskName.equalsIgnoreCase("sortedSet")){
            for(int y = 0;y<10;y++) {
                try (Jedis jedis = jedisPool.getResource()) {
                    for (int x = 0; x < taskNumber / 10; x++) {
                        jedis.zadd("z:voters"+x, y, (100 - (x % 23) + "vote"));
                        if (x % 60 == 0) {
                            jedis.info();
                        }
                    }
                }
            }
        }else if(taskName.equalsIgnoreCase("topK")){
            try(io.rebloom.client.Client topk = new Client(jedisPool)){
                topk.topkCreateFilter("top10voters",10,2000,10,.001);
                for(int x=0;x<taskNumber;x++){
                    topk.topkAdd("top10voters",(100-(x%23)+"vote"));
                }
            }
        }else{
            throw new NotImplementedException("sorry, no way to do that Hal...");
        }

    }
}