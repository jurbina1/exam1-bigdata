package edu.uprm.cse.bigdata.p1exam1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

    	 String list= "";

         for (Text value : values){
             list = list+value+", ";
         }
         
         //Logger loggerThing = LogManager.getRootLogger();

         // DEBUG
         context.write(key, new Text(list.substring(0, list.length()-2)));
    }

}
