import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.json.JSONObject;

public class Q4 {
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
		private LongWritable rtid = new LongWritable();
		private LongWritable uid = new LongWritable();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();

			if (!line.equals("")) {
				String retweeted_id = "";
				JSONObject obj = new JSONObject(line);
				JSONObject user = new JSONObject(obj.get("user").toString());
				String user_id = user.getString("id_str");
				if (obj.has("retweeted_status")) {

					JSONObject retweeted_status = new JSONObject(obj.get(
							"retweeted_status").toString());
					JSONObject retweeted_user = new JSONObject(retweeted_status
							.get("user").toString());
					retweeted_id = retweeted_user.getString("id_str");
					uid.set(Long.parseLong(user_id));
					rtid.set(Long.parseLong(retweeted_id));
					output.collect(rtid, uid);
				}

			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<LongWritable, LongWritable, LongWritable, Text> {

		public void configure(JobConf conf) {
			try {
				table = new HTable(conf, "q4");
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
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			ArrayList<Long> uid = new ArrayList<Long>();
			String sum = "";
			Put put = new Put(key.toString().getBytes());

			while (values.hasNext()) {
				Long tmp = values.next().get();

				if (!tmp.equals(null)) {
					if (!uid.contains(tmp)) {
						uid.add(tmp);
					}
				} else {
					System.out.println("null tmp");
				}

			}
			Collections.sort(uid);
			for (Long s : uid) {
				sum += s.toString();
				sum += ";";
			}
			output.collect(key, new Text(sum));
			put.add("tweet".getBytes(), "user_id".getBytes(), sum.getBytes());
			table.put(put);
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf jconf = new JobConf(Q4.class);
		jconf.setJobName("Q4");
		jconf.setOutputKeyClass(LongWritable.class);
		jconf.setOutputValueClass(LongWritable.class);
		jconf.setMapperClass(Map.class);
		jconf.setReducerClass(Reduce.class);
		jconf.setInputFormat(TextInputFormat.class);
		jconf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jconf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jconf, new Path(args[1]));
		JobClient.runJob(jconf);

	}
}
