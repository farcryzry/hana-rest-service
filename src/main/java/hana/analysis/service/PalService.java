package hana.analysis.service;

import hana.analysis.api.Pal;
import hana.analysis.db.Connector;
import hana.analysis.models.AnalysisResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class PalService {
	
	public static String Query(String tableName) {
		Connector c = new Connector();
		try {
			String s = c.Query(String.format("select * from %s", tableName));
			return s;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static AnalysisResult runKmeans(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("LIFESPEND", "DOUBLE");
		columns.put("NEWSPEND", "DOUBLE");
		columns.put("INCOME", "DOUBLE");
		columns.put("LOYALTY", "DOUBLE");

		String viewDef = "SELECT ID, LIFESPEND, NEWSPEND, INCOME, LOYALTY FROM PAL.CUSTOMERS";
		
		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);
		params.put("GROUP_NUMBER", 10);
		params.put("INIT_TYPE", 1);
		params.put("DISTANCE_LEVEL", 2);
		params.put("MAX_ITERATION", 100);
		params.put("NORMALIZATION", 0);
		params.put("EXIT_THRESHOLD", 1.0E-4);
		
		AnalysisResult result = Pal.kmeans(reGenerate, "PAL", "CUSTOMERS", columns, viewDef, params);
		System.out.println(result);
		
		return result;
	}
	
	public static AnalysisResult runNaiveBayes(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("POLICY", "VARCHAR(10)");
		columns.put("AGE", "INTEGER");
		columns.put("AMOUNT", "INTEGER");
		columns.put("OCCUPATION", "VARCHAR(10)");
		columns.put("FRAUD", "VARCHAR(10)");

		String viewDef = "SELECT POLICY, AGE, AMOUNT, OCCUPATION, FRAUD FROM PAL.CLAIMS";
		
		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);
		params.put("IS_SPLIT_MODEL", 0);
		params.put("LAPLACE", 0.01);
		
		AnalysisResult result = Pal.naiveBayes(reGenerate, "PAL", "CLAIMS", columns, viewDef, params);
		System.out.println(result);
		
		return result;
	}
	
	public static AnalysisResult runNaiveBayesPrediction(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("POLICY", "VARCHAR(10)");
		columns.put("AGE", "INTEGER");
		columns.put("AMOUNT", "INTEGER");
		columns.put("OCCUPATION", "VARCHAR(10)");
		
		List<List<Object>> data = new ArrayList<List<Object>>();
		data.add(Arrays.asList(new Object[] {1, "Travel", 41, 900, "IT"}));
		data.add(Arrays.asList(new Object[] {2, "Vehicle", 26, 6000, "Marketing"}));
		data.add(Arrays.asList(new Object[] {3, "Home", 54, 2500, "Marketing"}));
		data.add(Arrays.asList(new Object[] {4, "Vehicle", 33, 1200, "Marketing"}));
		data.add(Arrays.asList(new Object[] {5, "Home", 38, 2500, "Sales"}));
		
		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);
		
		AnalysisResult result = Pal.naiveBayesPredict(reGenerate, "PAL", "NBP_DATA", columns, data, "PAL.NBCTRAINRESULT1", params);
		System.out.println(result);
		
		return result;
	}
}
