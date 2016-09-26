package BDS6A1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalMedalsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	IntWritable tMedals;
	
	public void setup(Context context) {
		tMedals = new IntWritable();
	}
	
	public void reduce(Text key, Iterable<IntWritable> medals, Context context) throws IOException, InterruptedException {
		
		int sum=0;
		for(IntWritable medal:medals)
			sum=sum+medal.get();
		
		tMedals.set(sum);
		context.write(key,tMedals);
		
	}

}
