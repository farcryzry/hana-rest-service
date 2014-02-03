package hana.analysis.service;

import java.util.List;

import hana.analysis.models.*;

public class PredictOutputModel {
	List<AlgorithmParameter> params;
	ResultSetModel input;
	ResultSetModel predicted;
	
	public PredictOutputModel(AnalysisResult analysisResult, String sqlInput, String SqlPredicted) {
		this.params = analysisResult.getAlgorithm().getParams();
		if(sqlInput != null && sqlInput.length() > 0) this.input = PalService.Query(sqlInput);
		if(SqlPredicted != null && SqlPredicted.length() > 0) this.predicted = PalService.Query(SqlPredicted);
	}
}
