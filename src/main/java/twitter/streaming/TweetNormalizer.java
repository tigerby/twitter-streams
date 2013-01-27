package twitter.streaming;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class TweetNormalizer implements IRichBolt{
	private static final long serialVersionUID = -3025639777071957758L;

    static Logger LOG = Logger.getLogger(TweetNormalizer.class);

	static JSONParser jsonParser = new JSONParser();

    OutputCollector collector;
    Set<String> ignoreWords;
    String LANG;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Properties prop = new Properties();
        InputStream is = getClass().getResourceAsStream("/config.properties");
        try {
            prop.load(is);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        String ignoreWordStr = prop.getProperty("word.ignore");
        this.ignoreWords = new HashSet<String>(Arrays.asList(ignoreWordStr.split(",")));
        this.LANG =  prop.getProperty("word.lang");
    }

    @Override
    public void execute(Tuple tuple) {

        String json = (String)tuple.getValueByField("tweet");
        JSONObject jsonObject = null;

        try {
            jsonObject = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if(jsonObject.containsKey("text")){
            String textStr = (String) jsonObject.get("text");
            String location = (String)jsonObject.get("lang");
//            System.out.println(textStr);

            String[] words = textStr.split(" ");
            for(String word : words){
                word = word.trim();
                if(!word.isEmpty()){
                    word = word.toLowerCase();

                    List a = new ArrayList();
                    a.add(tuple);

                    if(this.LANG.equals(location) && !ignoreWords.contains(word)) {
                        collector.emit(a,new Values(word));
                    }
                }
            }

//            collector.ack(tuple);
        }
    }

    @Override
	public void cleanup() {
	}
 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


}
