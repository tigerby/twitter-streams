package twitter.streaming;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * To run this topology you should execute this main as: 
 * java -cp theGeneratedJar.jar twitter.streaming.Topology <track> <twitterUser> <twitterPassword>
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class SampleTopology {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new SampleApiStreamingSpout(),1);
		builder.setBolt("tweet-normalizer", new TweetNormalizer()).
			shuffleGrouping("tweets-collector"); 
		builder.setBolt("tweets-saver", new TweetKeywordCountSaver()).
			shuffleGrouping("tweet-normalizer");
		
		Config conf = new Config();
		int i = 0;
		conf.put("redisHost",args[i++]);
		conf.put("redisPort", new Integer(args[i++]));
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