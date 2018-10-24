package edu.uprm.cse.bigdata.p1exam1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject obj = new JSONObject(value.toString());
        // get the tweet
		String tweet = obj.getJSONObject("extended_tweet").getString("full_text");
        String tweetId = obj.getString("id_str");

        // now emit the following key-pair: keyword, 1
        if (tweet.toLowerCase().contains("flu")) {
        	context.write(new Text("Flu"), new Text(tweetId));
        }
        if(tweet.toLowerCase().contains("zika")) {
        	context.write(new Text("Zika"),new Text(tweetId));
        }
        if(tweet.toLowerCase().contains("diarrhea")) {
        	context.write(new Text("Diarrhea"), new Text(tweetId));
        }
        if(tweet.toLowerCase().contains("ebola")) {
        	context.write(new Text("Ebola"), new Text(tweetId));
        }
        if(tweet.toLowerCase().contains("swamp")) {
        	context.write(new Text("Swamp"), new Text(tweetId));
        }
        if(tweet.toLowerCase().contains("change")){
        	context.write(new Text("Change"), new Text(tweetId));
        }
	}
}
        
