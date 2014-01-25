package hana.analysis.service;

import hana.analysis.db.Connector;
import hana.analysis.models.Analysis;
import hana.analysis.models.AnalysisResult;
import hana.analysis.models.DataSource;
import hana.analysis.models.KmeansAlgorithm;

import java.sql.SQLException;
import java.util.LinkedHashMap;

public class Pal {

	public static AnalysisResult runKmeans() {
		Analysis analysis = new Analysis(new KmeansAlgorithm(), "PAL");

		LinkedHashMap<String, String> columns = new LinkedHashMap<String, String>();
		columns.put("ID", "INTEGER");
		columns.put("LIFESPEND", "DOUBLE");
		columns.put("NEWSPEND", "DOUBLE");
		columns.put("INCOME", "DOUBLE");
		columns.put("LOYALTY", "DOUBLE");
		DataSource source = new DataSource("PAL", "CUSTOMERS", columns,
				"SELECT ID, LIFESPEND, NEWSPEND, INCOME, LOYALTY FROM PAL.CUSTOMERS");

		AnalysisResult result = analysis.action(source);
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
