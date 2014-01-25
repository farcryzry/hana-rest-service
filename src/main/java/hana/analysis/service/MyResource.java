package hana.analysis.service;

import hana.analysis.models.AnalysisResult;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("myresource")
public class MyResource {

	/**
	 * Method handling HTTP GET requests. The returned object will be sent to
	 * the client as "text/plain" media type.
	 * 
	 * @return String that will be returned as a text/plain response.
	 */
	@Path("query/{table}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getIt(@PathParam("table") String tableName) {
		// return new Greeting(12121,"aa");
		// AnalysisResult result = Pal.runKmeans();
		// return result;
		String result = "";

		try {
			result = Pal.Query(tableName);
		} catch (Exception e) {
			result = e.getMessage();
		}

		return result;
	}
	
	@Path("kmeans")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult kmeans() {
		return Pal.runKmeans();
	}
}
