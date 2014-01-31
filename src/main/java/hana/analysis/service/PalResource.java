package hana.analysis.service;

import hana.analysis.models.AnalysisResult;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Root resource (exposed at "pal" path)
 */
@Path("pal")
public class PalResource {
	
	@Path("test")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String []  test() {

		return new String [] {"111", "222", "333"};
	}

	/**
	 * Method handling HTTP GET requests. The returned object will be sent to
	 * the client as "text/plain" media type.
	 * 
	 * @return String that will be returned as a text/plain response.
	 */
	@Path("query/table/{table}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String query(@PathParam("table") String tableName) {

		String result = "";

		try {
			result = PalService.Query(tableName);
		} catch (Exception e) {
			result = e.getMessage();
		}

		return result;
	}
	
	@Path("kmeans")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult kmeans() {
		return PalService.runKmeans(true);
	}
	
	@Path("naivebayes")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult naivebayes() {
		return PalService.runNaiveBayes(true);
	}
	
	@Path("nbpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult nbpredict() {
		return PalService.runNaiveBayesPrediction(true);
	}
	
	@Path("svm")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult svm() {
		return PalService.runSupportVectorMachine(true);
	}
	
	@Path("svmpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult svmpredict() {
		return PalService.runSupportVectorMachinePrediction(true);
	}
	
	@Path("knn")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult knn() {
		return PalService.runKnn(true);
	}
	
	@Path("lr")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult lr() {
		return PalService.runLogisticRegression(true);
	}
	
	@Path("lrpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult lrpredict() {
		return PalService.runLogisticRegressionPrediction(true);
	}
}
