package hana.analysis.service;

import hana.analysis.api.Pal;
import hana.analysis.db.Connector;
import hana.analysis.models.AnalysisResult;
import hana.analysis.models.ResultSetModel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;

public class PalService {

	public static ResultSetModel Query(String sql) {
		Connector c = new Connector();
		try {
			ResultSetModel s = c.Query(sql);
			return s;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ResultSetModel QueryTable(String tableName) {
		return Query(String.format("select * from %s", tableName));
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

		AnalysisResult result = Pal.kmeans(reGenerate, "PAL", "CUSTOMERS",
				columns, viewDef, params);
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

		AnalysisResult result = Pal.naiveBayes(reGenerate, "PAL", "CLAIMS",
				columns, viewDef, params);
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
		data.add(Arrays.asList(new Object[] { 1, "Travel", 41, 900, "IT" }));
		data.add(Arrays.asList(new Object[] { 2, "Vehicle", 26, 6000,
				"Marketing" }));
		data.add(Arrays
				.asList(new Object[] { 3, "Home", 54, 2500, "Marketing" }));
		data.add(Arrays.asList(new Object[] { 4, "Vehicle", 33, 1200,
				"Marketing" }));
		data.add(Arrays.asList(new Object[] { 5, "Home", 38, 2500, "Sales" }));

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);

		AnalysisResult result = Pal.naiveBayesPredict(reGenerate, "PAL",
				"NBP_DATA", columns, data, "PAL.NBCTRAINRESULT1", params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runSupportVectorMachine(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("FRAUD", "INTEGER");
		columns.put("POLICY", "VARCHAR(10)");
		columns.put("AGE", "INTEGER");
		columns.put("AMOUNT", "INTEGER");
		columns.put("OCCUPATION", "INTEGER");

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);
		params.put("KERNEL_TYPE", 2);
		params.put("TYPE", 1);

		AnalysisResult result = Pal.supportVectorMachine(reGenerate, "PAL",
				"FRAUD", columns, null, params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runSupportVectorMachinePrediction(
			boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("POLICY", "VARCHAR(10)");
		columns.put("AGE", "INTEGER");
		columns.put("AMOUNT", "INTEGER");
		columns.put("OCCUPATION", "VARCHAR(10)");

		List<List<Object>> data = new ArrayList<List<Object>>();
		data.add(Arrays.asList(new Object[] { 1, 2, 41, 900, 2 }));
		data.add(Arrays.asList(new Object[] { 2, 3, 26, 6000, 3 }));
		data.add(Arrays.asList(new Object[] { 3, 1, 54, 2500, 2 }));
		data.add(Arrays.asList(new Object[] { 4, 2, 33, 1200, 3 }));
		data.add(Arrays.asList(new Object[] { 5, 1, 38, 2800, 1 }));

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);

		List<String> ModelTables = new ArrayList<String>();
		ModelTables.add("PAL.SVMTRAINRESULT1");
		ModelTables.add("PAL.SVMTRAINRESULT2");

		AnalysisResult result = Pal.supportVectorMachinePredict(reGenerate,
				"PAL", "SVP_DATA", columns, data, ModelTables, params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runKnn(boolean reGenerate) {
		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("CUSTOMER_ID", "INTEGER");
		columns.put("GENDER", "INTEGER");
		columns.put("LIFESPEND", "INTEGER");
		columns.put("NEWSPEND", "INTEGER");

		List<List<Object>> classData = new ArrayList<List<Object>>();
		classData.add(Arrays.asList(new Object[] { 1, 3020, 1300 }));
		classData.add(Arrays.asList(new Object[] { 2, 5300, 980 }));

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 4);
		params.put("ATTRIBUTE_NUM", 2);
		params.put("K_NEAREST_NEIGHBOURS", 3);
		params.put("VOTING_TYPE", 0);
		params.put("METHOD", 0);

		String viewDef = "SELECT c.CUSTOMER_ID, c.CUSTOMER_GENDER_ID AS GENDER, l.LIFESPEND, n.NEWSPEND FROM PAL.CUSTOMER c"
				+ " INNER JOIN ("
				+ " SELECT CUSTOMER_ID, SUM(SALES_AMOUNT) AS LIFESPEND"
				+ " FROM PAL.ORDER_FACTS"
				+ " GROUP BY CUSTOMER_ID"
				+ " ) l ON(c.CUSTOMER_ID = l.CUSTOMER_ID)"
				+ " INNER JOIN ("
				+ " SELECT CUSTOMER_ID, SUM(SALES_AMOUNT) AS NEWSPEND"
				+ " FROM PAL.ORDER_FACTS"
				+ " WHERE CALENDAR_ID=23"
				+ " GROUP BY CUSTOMER_ID"
				+ " ) n ON(c.CUSTOMER_ID = n.CUSTOMER_ID)";

		AnalysisResult result = Pal.kNearestNeighbor(reGenerate, "PAL",
				"SVP_DATA", columns, viewDef, "PAL.KNN_CLASS", classData,
				params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runLogisticRegression(boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("LIFESPEND", "INTEGER");
		columns.put("GENDER", "INTEGER");

		String viewDef = "SELECT TOP 1000 l.LIFESPEND, c.CUSTOMER_GENDER_ID AS GENDER"
				+ " FROM PAL.CUSTOMER c"
				+ " INNER JOIN ("
				+ " SELECT CUSTOMER_ID, SUM(SALES_AMOUNT) AS LIFESPEND"
				+ " FROM PAL.ORDER_FACTS"
				+ " GROUP BY CUSTOMER_ID"
				+ " ) l ON(c.CUSTOMER_ID = l.CUSTOMER_ID)";

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 4);
		params.put("MAX_ITERATION", 100);
		params.put("EXIT_THRESHOLD", 0.00001);
		params.put("VARIABLE_NUM", 1);
		params.put("METHOD", 0);
		params.put("PMML_EXPORT", 2);

		AnalysisResult result = Pal.logisticRegression(reGenerate, "PAL",
				"CUSTOMER", columns, viewDef, params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runLogisticRegressionPrediction(
			boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("LIFESPEND", "INTEGER");

		List<List<Object>> data = new ArrayList<List<Object>>();
		data.add(Arrays.asList(new Object[] { 1, 3000 }));
		data.add(Arrays.asList(new Object[] { 2, 5000 }));
		data.add(Arrays.asList(new Object[] { 3, 9000 }));
		data.add(Arrays.asList(new Object[] { 4, 12000 }));
		data.add(Arrays.asList(new Object[] { 5, 14000 }));

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);

		AnalysisResult result = Pal.logisticRegressionPredict(reGenerate,
				"PAL", "RGP_PREDICT", columns, data,
				"PAL.LOGISTICREGRESSIONRESULT1", params);
		System.out.println(result);

		return result;
	}

	public static AnalysisResult runMultipleLinearRegression(
			boolean reGenerate, int variableNum, int pmmlExport) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("LIFESPEND", "DOUBLE");
		columns.put("INCOME", "DOUBLE");
		columns.put("LOYALTY", "DOUBLE");
		columns.put("NEWSPEND", "DOUBLE");

		String viewDef = "SELECT ID, LIFESPEND, INCOME, LOYALTY, NEWSPEND FROM PAL.CUSTOMERS";

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);
		params.put("VARIABLE_NUM", variableNum <= 0 ? 3 : variableNum);
		params.put("PMML_EXPORT", pmmlExport <= 0 ? 2 : pmmlExport);

		AnalysisResult result = Pal.multipleLinearRegression(reGenerate, "PAL",
				"CUSTOMERS", columns, viewDef, params);
		System.out.println(result);

		return result;
	}

	public static PredictOutputModel runMultipleLinearRegressionPrediction(
			boolean reGenerate) {

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("INCOME", "DOUBLE");
		columns.put("LOYALTY", "DOUBLE");
		columns.put("NEWSPEND", "DOUBLE");

		List<List<Object>> data = new ArrayList<List<Object>>();
		data.add(Arrays.asList(new Object[] { 151, 2.5, 3.1, 4.5 }));
		data.add(Arrays.asList(new Object[] { 152, 7.5, 4.2, 7.6 }));

		LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
		params.put("THREAD_NUMBER", 2);

		AnalysisResult result = Pal.multipleLinearRegressionPredict(reGenerate,
				"PAL", "MLRGP_PREDICT", columns, data,
				"PAL.LRREGRESSIONRESULT1", params);
		System.out.println(result);

		String sqlInput = "SELECT ID, LIFESPEND, INCOME, LOYALTY, NEWSPEND FROM PAL.CUSTOMERS";
		String sqlPredict = "SELECT a.ID, ROUND(a.FITTED,1) AS LIFESPEND, "
				+ " b.INCOME, b.LOYALTY, b.NEWSPEND FROM PAL.FORECASTWITHLRRESULT1 a "
				+ " INNER JOIN PAL.MLRGP_PREDICT b ON (a.ID=b.ID)";
		PredictOutputModel predictOutputModel = new PredictOutputModel(result,
				sqlInput, sqlPredict);

		return predictOutputModel;
	}

}
