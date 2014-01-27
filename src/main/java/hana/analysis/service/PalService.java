package hana.analysis.service;

import hana.analysis.api.Pal;
import hana.analysis.db.Connector;
import hana.analysis.models.AnalysisResult;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class PalService {

	public static AnalysisResult runKmeans(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("LIFESPEND", "DOUBLE");
		columns.put("NEWSPEND", "DOUBLE");
		columns.put("INCOME", "DOUBLE");
		columns.put("LOYALTY", "DOUBLE");

		String viewDef = "SELECT ID, LIFESPEND, NEWSPEND, INCOME, LOYALTY FROM PAL.CUSTOMERS";
		
		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 1);
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
		params.put("THREAD_NUMBER", 1);
		params.put("IS_SPLIT_MODEL", 0);
		params.put("LAPLACE", 0.01);
		
		AnalysisResult result = Pal.naiveBayes(reGenerate, "PAL", "CLAIMS", columns, viewDef, params);
		System.out.println(result);
		
		return result;
	}

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
}
