package ca.ulaval.graal.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The famous MapReduce word count example for Hadoop.
 */
public class MaxTemperatureDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MaxTemperatureDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] remainingArgs = new GenericOptionsParser(getConf(), args)
				.getRemainingArgs();

		if (remainingArgs.length < 2) {
			System.err.println("Usage: Weather <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}

		Job job = new Job(getConf(), "Weather");
		job.setJarByClass(getClass());

		job.setMapperClass(YearTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
