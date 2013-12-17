package rest.api;

import java.io.IOException;
import java.util.List;

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


@Path("/q4")
public class WhoRetweeted {
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
	public Response getQ4(@QueryParam("userid") String userid) throws IOException {
		System.out.println(userid);
		String output = "CoolShine, 7869-4661-0595\n" + getResult("q4", userid);
		System.out.println(output);
		return Response.status(200).entity(output).build();
	}


	public String getResult(String tablename, String rowKey)
			throws IOException {
		String output = "";

		HTable table = new HTable(conf, tablename);
		Get g = new Get(rowKey.getBytes());
		Result rs = table.get(g);
		for (KeyValue kv : rs.raw()) {
			output += new String(kv.getValue()).replace(";", "\n");
		}
		table.close();
		
		return output;
	}
}
