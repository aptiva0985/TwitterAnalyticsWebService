import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.json.JSONObject;

public class Q3 {
	static Configuration conf = null;
	static HTable table = null;
	static {
		conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.zookeeper.quorum", "107.21.182.185");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master",
				"ec2-107-21-182-185.compute-1.amazonaws.com:60000");
		conf.set("hbase.rootdir", "hdfs://107.21.182.185:9000/hbase");
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, LongWritable> {
		private LongWritable uid = new LongWritable();
		private LongWritable tid = new LongWritable();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();

			if (!line.equals("")) {

				JSONObject obj = new JSONObject(line);
				String id = obj.getString("id_str");

				JSONObject user = new JSONObject(obj.get("user").toString());
				String user_id = user.getString("id_str");
				uid.set(Long.parseLong(user_id));
				tid.set(Long.parseLong(id));

				output.collect(uid, tid);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		public void configure(JobConf conf) {

			try {
				table = new HTable(conf, "q3");
				table.setAutoFlush(false);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void close(JobConf conf) throws IOException {
			table.close();
		}

		@Override
		public void reduce(LongWritable key, Iterator<LongWritable> values,
				OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) throws IOException {

			Put put = new Put(key.toString().getBytes());
			ArrayList<Long> sum = new ArrayList<Long>();
			Long count = (long) 0;

			while (values.hasNext()) {
				Long tmp = Long.parseLong(values.next().toString().trim());
				if (!tmp.equals(null)) {
					if (!sum.contains(tmp)) {
						count++;
						sum.add(tmp);
					}
				}
			}

			output.collect(key, new LongWritable(count));
			put.add("user".getBytes(), "id".getBytes(), Long.toString(count)
					.getBytes());
			table.put(put);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Q3.class);
		conf.setJobName("Q3");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
