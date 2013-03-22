package com.tiger.bolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.Map;

/**
 * <p> title here </p>
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bryan</a>
 * @version 1.0
 */
public class LocationRetriever extends BaseRichBolt {
    static JSONParser jsonParser = new JSONParser();
    static final String GOOGLE_GEO_URL = "http://maps.googleapis.com/maps/api/geocode/json?sensor=false&latlng=";

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String tweet = input.getStringByField("tweet");
        JSONObject jsonObject = null;
        Object geo = null;

        try {
            jsonObject = (JSONObject)jsonParser.parse(tweet);

            JSONObject geoObj = (JSONObject)jsonObject.get("geo");
            if(geoObj != null &&  geoObj.containsKey("coordinates")) {
                JSONArray array = (JSONArray)geoObj.get("coordinates");
                Double latitude = (Double)array.get(0);
                Double longitude = (Double)array.get(1);

                HttpClient client = new DefaultHttpClient();
                HttpGet httpGet = new HttpGet(GOOGLE_GEO_URL + latitude + "," + longitude);

                HttpResponse response = null;
                try {
                    response = client.execute(httpGet);
                    StatusLine statusLine = response.getStatusLine();
                    if(statusLine.getStatusCode() == 200) {
                        InputStream inputStream = response.getEntity().getContent();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        StringBuffer sb = new StringBuffer();
                        String in;
                        while((in = bufferedReader.readLine()) != null) {
                            sb.append(in);
                        }

                        JSONObject countryObject = findCountryJSONObject((JSONObject)jsonParser.parse(sb.toString()));
                        String country = (String) countryObject.get("formatted_address");
                        collector.emit(new Values(country, tweet));

                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            } else {
                collector.emit(new Values("no location", tweet));
            }

        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country", "tweet"));
    }

    public JSONObject findCountryJSONObject(JSONObject jsonObject) {
        JSONArray array = (JSONArray)jsonObject.get("results");
        for(int i=0;i<array.size();i++) {
            JSONObject addressObj = (JSONObject)array.get(i);
            JSONArray typeArray =  (JSONArray)addressObj.get("types");
            for(int j=0; j<typeArray.size();j++) {
                String type = (String)typeArray.get(j);
                if("country".equals(type)) {
                    return addressObj;
                }
            }
        }
        return null;
    }
}
