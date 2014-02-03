package hana.analysis.service;

import java.util.List;

import hana.analysis.models.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Root resource (exposed at "pal" path)
 */
@Path("pal")
public class PalResource {
	
	@Path("test")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String []  test(@QueryParam("p1") String p1) {

		return new String [] {p1 != null ? p1 : "111", "222", "333"};
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
	
	/*
	@Path("mlr")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public AnalysisResult mlr() {
		return PalService.runMultipleLinearRegression(false, 3, 2);
	}
	*/
	
	@Path("mlrpredict_meta")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public List<AlgorithmParameter> mlrpredictMeta() {
		Algorithm mlrAlgorithm = new MlrAlgorithm();
		return mlrAlgorithm.getParams();
	}
	
	@Path("mlrpredict")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public PredictOutputModel mlrpredict(@QueryParam("reGenerate") boolean reGenerate, @QueryParam("VARIABLE_NUM")int variableNum, @QueryParam("PMML_EXPORT")int pmmlExport) {
		PalService.runMultipleLinearRegression(reGenerate, variableNum, pmmlExport);
		return PalService.runMultipleLinearRegressionPrediction(reGenerate);
	}
}
