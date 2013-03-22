package com.tiger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.tiger.bolt.WordNormalizer;
import com.tiger.spout.SampleApiStreamingSpout;
import com.tiger.bolt.TweetKeywordCountSaver;

/**
 * To run this topology you should execute this main as: 
 * java -cp theGeneratedJar.jar twitter.streaming.Topology <track> <twitterUser> <twitterPassword>
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class WordInTweetCountTopology {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new SampleApiStreamingSpout(), 2);
		builder.setBolt("tweet-normalizer", new WordNormalizer(), 4).
			shuffleGrouping("tweets-collector"); 
		builder.setBolt("tweets-saver", new TweetKeywordCountSaver(), 4).
			shuffleGrouping("tweet-normalizer");
		
		Config conf = new Config();
		int i = 0;
		conf.put("infinispanHost",args[i++]);
		conf.put("infinispanPort", new Integer(args[i++]));
		conf.put("user", args[i++]);
		conf.put("password", args[i++]);
		i++;

		if(args.length <= i || args[i].equals("local")){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitter-sample-summarizer", conf, builder.createTopology());
		}else{
			StormSubmitter.submitTopology("twitter-sample-summarizer", conf, builder.createTopology());
		}
	}
}
