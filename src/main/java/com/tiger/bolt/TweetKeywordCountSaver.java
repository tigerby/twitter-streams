package com.tiger.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;
import backtype.storm.utils.TimeCacheMap.ExpiredCallback;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class TweetKeywordCountSaver implements IRichBolt, ExpiredCallback<String, Integer>{

	Map<String, Integer> hashtags = new HashMap<String, Integer>();
	private Jedis jedis;
	TimeCacheMap<String, Integer> cacheMap;

    Integer id;
    String name;
    private OutputCollector collector;


    @Override
	public void cleanup() {
//        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
//        for(Map.Entry<String, Integer> entry : cacheMap.entrySet()){
//            System.out.println(entry.getKey()+": "+entry.getValue());
//        }
    }

	@Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);


        // If the word dosn't exist in the map we will create * this, if not We will add 1
        if(!cacheMap.containsKey(str)){
            cacheMap.put(str, 1);
        }else{
            Integer c = cacheMap.get(str) + 1; 
            cacheMap.put(str, c);
        }
        
//        //Set the tuple as Acknowledge
//        collector.ack(input);
	}

	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

		String infinispanHost = (String) conf.get("infinispanHost");
		Integer infinispanPort = ((Long) conf.get("infinispanPort")).intValue();
		this.jedis = new Jedis(infinispanHost, infinispanPort);
		this.jedis.connect();
		cacheMap = new TimeCacheMap<String, Integer>(10, this);
	}


	@Override
	public void expire(String key, Integer val) {
        System.out.println(key + val);
        jedis.hincrBy("hashs", key, val);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

}
