package hana.analysis.service;

import hana.analysis.models.AnalysisResult;
import hana.analysis.models.ResultSetModel;

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
	public ResultSetModel queryTable(@PathParam("table") String tableName) {

		ResultSetModel result = null;

		try {
			result = PalService.QueryTable(tableName);
		} catch (Exception e) {
			//result = e.getMessage();
		}

		return result;
	}
	
	@Path("query/{sql}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public ResultSetModel query(@PathParam("sql") String sql) {

		ResultSetModel result = null;

		try {
			result = PalService.Query(sql);
		} catch (Exception e) {
			//result = e.getMessage();
		}

		return result;
	}
	
	@Path("kmeans")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult kmeans() {
		return PalService.runKmeans(false);
	}
	
	@Path("naivebayes")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult naivebayes() {
		return PalService.runNaiveBayes(false);
	}
	
	@Path("nbpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult nbpredict() {
		return PalService.runNaiveBayesPrediction(false);
	}
	
	@Path("svm")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult svm() {
		return PalService.runSupportVectorMachine(false);
	}
	
	@Path("svmpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult svmpredict() {
		return PalService.runSupportVectorMachinePrediction(false);
	}
	
	@Path("knn")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult knn() {
		return PalService.runKnn(false);
	}
	
	@Path("lr")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult lr() {
		return PalService.runLogisticRegression(false);
	}
	
	@Path("lrpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult lrpredict() {
		return PalService.runLogisticRegressionPrediction(false);
	}
	
	@Path("mlr")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult mlr() {
		return PalService.runMultipleLinearRegression(false);
	}
	
	@Path("mlrpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public PredictOutputModel mlrpredict() {
		return PalService.runMultipleLinearRegressionPrediction(false);
	}
}
