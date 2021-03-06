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
 *
 * @author StormBook
 *
 */
public class Topology {
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new ApiStreamingSpout(),1);
		builder.setBolt("data-extractor", new TwitterDataExtractor()).
			shuffleGrouping("tweets-collector"); 
		builder.setBolt("tweets-saver", new TwitterHashtagsSaver()).
			shuffleGrouping("data-extractor");
		
		Config conf = new Config();
		int i = 0;
		conf.put("infinispanHost",args[i++]);
		conf.put("infinispanPort", new Integer(args[i++]));
		conf.put("track", args[i++]);
		conf.put("user", args[i++]);
		conf.put("password", args[i++]);
		i++;
		if(args.length <= i || args[i].equals("local")){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitter-hashtag-summarizer", conf, builder.createTopology());
		}else{
			StormSubmitter.submitTopology("twitter-hashtag-summarizer", conf, builder.createTopology());
		}
	}
}
