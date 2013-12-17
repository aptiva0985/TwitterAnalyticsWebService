package rest.api;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

@Path("/q3")
public class NumberOfTweets {
	static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.zookeeper.quorum", "107.21.182.185");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "ec2-107-21-182-185.compute-1.amazonaws.com:60000");
		conf.set("hbase.rootdir", "hdfs://107.21.182.185:9000/hbase");
	}

	@GET
	public Response getQ3(@QueryParam("userid_min") String userid_min, @QueryParam("userid_max") String userid_max) {
		String output = "CoolShine, 7869-4661-0595\n" + getResult("q3", userid_min, userid_max);
		System.out.println(output);
		return Response.status(200).entity(output).build();
	}

	public String getResult(String tablename, String userid_min, String userid_max) {
		long output = 0;
		HTable table = null;
		ResultScanner rs = null;
		try {	
			table = new HTable(conf, tablename.getBytes());
			Scan scan =  new Scan();
			scan.setStartRow(userid_min.getBytes());
			scan.setStopRow(String.valueOf(Long.parseLong(userid_max) + 1).getBytes());
			rs = table.getScanner(scan);
			Result next = rs.next();
			while (next != null) {
				for(KeyValue kv : next.raw()){
					if (new String(kv.getRow()).length() == userid_min.length())
						output += Long.parseLong(new String(kv.getValue()));
				}
				next = rs.next();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			rs.close();
			try {
				table.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		return String.valueOf(output) + "\n";
	}
}
