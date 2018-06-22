package com.rmpc.marklogictest;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.ExtractedItem;
import com.marklogic.client.query.ExtractedResult;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;

/**
 * @author Rui Carrilho
 *
 */
public class ExtractExample {
	// replace with your MarkLogic Server connection information
	static String HOST = "127.0.0.1";
	static int PORT = 8011;
	static String USER = "ruicarrilho";
	static String PASSWORD = "";
	static DatabaseClient client = DatabaseClientFactory.newClient(HOST, PORT, new DigestAuthContext(USER, PASSWORD));
	static String DIR = "/extract/";

	// Insert some example documents in the database.
	public static void setup() {
		StringHandle jsonContent = new StringHandle("{\"parent\": {" + "\"a\": \"foo\"," + "\"body\": {"
				+ "\"target\": \"content1\"" + "}," + "\"b\": \"bar\"" + "}}").withFormat(Format.JSON);
		GenericDocumentManager gdm = client.newDocumentManager();

		DocumentWriteSet batch = gdm.newWriteSet();
		batch.add(DIR + "doc1.json", jsonContent);
		gdm.write(batch);
	}

	// Perform a search with RawCombinedQueryDefinition that extracts
	// just the "target" element or property of docs in DIR.
	public static void example() {
	    String rawQuery = 
	        "<search xmlns=\"http://marklogic.com/appservices/search\">" +
	        "  <query>" +
	        "    <directory-query><uri>" + DIR + "</uri></directory-query>" +
	        "  </query>" +
	        "  <options>" +
	        "    <extract-document-data selected=\"include\">" +
	        "      <extract-path>/parent/body/target</extract-path>" +
	        "    </extract-document-data>" +
	        "  </options>" +
	        "</search>";
		StringHandle qh = new StringHandle(rawQuery).withFormat(Format.XML);

		QueryManager qm = client.newQueryManager();
		RawCombinedQueryDefinition query = qm.newRawCombinedQueryDefinition(qh);

		SearchHandle results = qm.search(query, new SearchHandle());

		System.out.println("Total matches: " + results.getTotalResults());

		MatchDocumentSummary matches[] = results.getMatchResults();
		for (MatchDocumentSummary match : matches) {
			System.out.println("Extracted from uri: " + match.getUri());
			ExtractedResult extracts = match.getExtracted();
			for (ExtractedItem extract : extracts) {
				System.out.println("  extracted content: " + extract.getAs(String.class));
			}
		}
	}

	// Delete the documents inserted by setup.
	public static void teardown() {
		QueryManager qm = client.newQueryManager();
		DeleteQueryDefinition byDir = qm.newDeleteDefinition();
		byDir.setDirectory(DIR);
		qm.delete(byDir);
	}

	public static void main(String[] args) {
		setup();
		example();
		teardown();
	}
}
