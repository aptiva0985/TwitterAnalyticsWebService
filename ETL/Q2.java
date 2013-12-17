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

public class Q2 {
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
			Mapper<LongWritable, Text, Text, Text> {
		private Text time = new Text();
		private Text id_text = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();

			if (!line.equals("")) {

				int tindex = line.indexOf("\"text\":");
				int sindex = line.indexOf("\"source\":");
				if (tindex > sindex) {
					System.out.println("tindex > sindex!");
				} else if (line.indexOf("\"source\":", sindex + 1) != -1) {
					System.out.println(line);
				}
				String text = line.substring(tindex + 8, sindex - 2);

				JSONObject obj = new JSONObject(line);

				String id = obj.getString("id_str");
				String created_at = obj.getString("created_at");
				String[] times = created_at.split(" ");
				String t = "2013-10-" + times[2] + " " + times[3];

				time.set(t);
				id_text.set(id + ":" + text);
				output.collect(time, id_text);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void configure(JobConf conf) {
			try {
				table = new HTable(conf, "q2");
				table.setAutoFlush(false);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void close(JobConf conf) throws IOException {
			table.close();
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Put put = new Put(key.toString().getBytes());
			ArrayList<IdText> id_text = new ArrayList<IdText>();
			ArrayList<String> filter = new ArrayList<String>();
			String sum = "";

			while (values.hasNext()) {
				String tmp = values.next().toString().trim();
				String[] id_split = tmp.split(":");

				if (id_split.length > 0) {

					if (!filter.contains(id_split[0])) {
						filter.add(id_split[0]);
						IdText row = new IdText(Long.parseLong(id_split[0]),
								tmp);
						if (!id_text.contains(row)) {
							id_text.add(row);
						}
					}
				} else {
					System.out.println("split length < 0!");
					System.out.println("value:" + tmp);
				}
			}
			Collections.sort(id_text, new CustomComparator());
			for (IdText i : id_text) {
				sum += i.ID_Text;
				sum += "\n";
			}
			output.collect(key, new Text(sum));
			put.add("tweet".getBytes(), "id_text".getBytes(), sum.getBytes());
			table.put(put);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Q2.class);
		conf.setJobName("Q2");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
