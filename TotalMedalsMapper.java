package BDS6A1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalMedalsMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	Text country;
	IntWritable medals;
	String[] line;
	
	public void setup(Context context) {
		
		country = new Text();
		medals = new IntWritable();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		line = value.toString().split("\t");
		
		if (line[5].matches("Swimming")) {
			country.set(line[2]);
			medals.set(Integer.parseInt(line[9]));
			context.write(country, medals);
		}
	}

}
