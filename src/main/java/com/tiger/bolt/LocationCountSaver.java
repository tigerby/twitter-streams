package com.tiger.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * <p> title here </p>
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class LocationCountSaver extends BaseRichBolt {
    int id;
    String name;

    Map<String, Integer> counter = new HashMap<String, Integer>();
    OutputCollector collector;
    int cnt;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.id = context.getThisTaskId();
        this.name = context.getThisComponentId();
        this.collector = collector;


    }

    @Override
    public void execute(Tuple input) {
        String country = input.getStringByField("country");

        if(counter.containsKey(country)) {
            counter.put(country, counter.get(country)+1);
        } else {
            counter.put(country, 1);
        }

        System.out.println("----counter " + this.id + ", " + this.name + ", " +  ++cnt + " -------");
        System.out.println(counter);
        System.out.println("----counter -------");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }
}
