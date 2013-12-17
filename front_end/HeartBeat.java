package rest.api;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Path("/q1")
public class HeartBeat {
	@GET	
	public Response getQ1() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date t = new Date(System.currentTimeMillis());
		String output = "CoolShine, 7869-4661-0595\n" + format.format(t);
		return Response.status(200).entity(output).build();
	}
}
