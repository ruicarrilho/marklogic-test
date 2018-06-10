package com.rmpc.marklogictest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentPatchBuilder;
import com.marklogic.client.document.DocumentPatchBuilder.Position;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonDatabindHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.DocumentPatchHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;

/**
 1) create a database
 
 2) setup a forest which maps where the documents are stored in the file system - can have many of those
    naming convention is usually database_name-01, database_name-02,  database_name-0n

 3) setup a port that allows a program to communicate with marklogic database - java client is built on top of the REST API
    so a rest api instance is required when using the java api

 command:
 curl --anyauth --user admin:admin -X POST -d@"news-config.json" -i -H
 "Content-type: application/json"
 http://localhost:8002/v1/rest-api

 json config file:
 {
   "rest-api": {
     "name": "8011-news-rest",
     "database": "news",
     "port": "8011"
   }
 }

 *
 */
public class App {

	private static final String HOST = "127.0.0.1";
	private static final int PORT = 8011;
	private static final String DATABASE = "news";
	private static final String USERNAME = "ruicarrilho";
	private static final String PASSWORD = "";
	private static final String DATA_DIR = "/your/input/dir/";
	private static final String EX_DIR = "/your/directory/here";
	
	private static DatabaseClient client;
	private static JSONDocumentManager jsonDocMgr;
	private static TextDocumentManager textDocMgr;
	private static QueryManager queryMgr;

	/**
	 * client & managers are thread safe
	 */
	static {
		client = DatabaseClientFactory.newClient(HOST, PORT, DATABASE,
				new DigestAuthContext(USERNAME, PASSWORD));
		jsonDocMgr = client.newJSONDocumentManager();
		textDocMgr = client.newTextDocumentManager();
		queryMgr = client.newQueryManager();
	}

	/**
	 * inserting a document with write() method
	 */
	private static void createDocument(String uri, String content) {
		jsonDocMgr.write(uri, new StringHandle(content));
	}

	/**
	 * Having marklogic generate the uri for us. We just need to let marklogic know its a json document
	 * @param content
	 */
	private static void createDocumentAndURI(String content) {
		 jsonDocMgr.create(jsonDocMgr.newDocumentUriTemplate("json"),
				 new StringHandle(content));
	}

	/**
	 * to write to a particular collection we need to specify it with a meta data handle as bellow
	 * @param uri
	 * @param content
	 */
	private static void createTextDocument(String uri, String content) {
		// Create a handle to hold string content.
		StringHandle handle = new StringHandle();
		DocumentMetadataHandle defaultMetadata =
				new DocumentMetadataHandle().withCollections("April 2014");

		// Give the handle some content
		handle.set(content);
		// Write the document to the database with URI from docId and content from handle
		textDocMgr.write(uri, defaultMetadata, handle);
	}
	
	/**
	 * write multiple docs synchronously - if any doc fails to write the whole batch fails as a transaction 
	 * @param docsMap
	 */
	private static void createMultipleDocuments(Map<String, String> docsMap) {
		DocumentWriteSet batch = jsonDocMgr.newWriteSet();
		for (String uri : docsMap.keySet()) {
			batch.add(uri, new StringHandle(docsMap.get(uri)));
		}
		jsonDocMgr.write(batch);
	}
	
	private static void readMultipleDocuments(Map<String, String> docsMap) {
		List<String> uris = docsMap.keySet().stream().map(d -> d).collect(Collectors.toList());
		DocumentPage page = jsonDocMgr.read(uris.stream().toArray(String[]::new));
		while (page.hasNext()) {
			DocumentRecord document = page.next();
			StringHandle handle = new StringHandle();
			String doc = document.getContent(handle).get();
			System.out.println(doc);			
		}
	}

	/**
	 * read using the read() method via the uri
	 * @param uri
	 */
	private static void readDocument(String uri, String context) {
		String doc = jsonDocMgr.read(uri, new StringHandle()).get();
		System.out.println(context);
		System.out.println(doc);		
	}
	
	/**
	 * patching a document - needs to be an object node
	 * 
	 * @param uri - document uri that we want to update
	 * @param patchContext - property that will be updated, needs to be an object
	 * @param patchContent - content being inserted in the patched property
	 */
	private static void patchDocument(String uri, String patchContext, String patchContent) {
		DocumentPatchBuilder patchBldr = jsonDocMgr.newPatchBuilder();
		DocumentPatchHandle patch = patchBldr
				.insertFragment(patchContext, Position.LAST_CHILD, patchContent)
				.build();

		jsonDocMgr.patch(uri, patch);
	}
	
	/**
	 * to delete a document we just call the delete() method with the uri of the document
	 * @param uri
	 */
	private static void deleteDocument(String uri) {
		jsonDocMgr.delete("/afternoon-drink");
	}
	
	/**
	 * 
	 * @param searchCriteria - keywords to search
	 */
	private static void searchDocument(String searchCriteria) {
		SearchHandle searchHandle = new SearchHandle();
		// search - query
		StringQueryDefinition query = queryMgr.newStringDefinition();
		query.setCriteria(searchCriteria);
		// query.setCollections("drinks"); // if we want to search in a particular collection of the database
		//SearchHandle results = queryMgr.search(query, searchHandle);
//		JacksonDatabindHandle<Drink> results = queryMgr.search(query, new JacksonDatabindHandle<>(Drink.class));
		StringHandle results = queryMgr.search(query, new StringHandle().withFormat(Format.JSON));
		System.out.println(results.get());
	}

	private static void structedSearchDocument(String searchCriteria) {
		StructuredQueryBuilder qb = queryMgr.newStructuredQueryBuilder();
		SearchHandle handle = new SearchHandle();
		StructuredQueryDefinition query = qb.and(
				qb.word(qb.jsonProperty("name"), "manhanthan"));
		SearchHandle results = queryMgr.search(query, handle);
		parseResults(results);
		
		SearchHandle handle1 = new SearchHandle();
		
		results = queryMgr.search(
				  new StructuredQueryBuilder().term("true", "Mocha"),
				  handle1);
		parseResults(results);
	}

	private static void parseResults(SearchHandle results) {
		for (MatchDocumentSummary summary : results.getMatchResults()) {
            System.out.println("  * found {} "  + summary.getUri());
            // Assumption: summary URI refers to JSON document
            JacksonDatabindHandle<Drink> jacksonHandle = new JacksonDatabindHandle<Drink>(Drink.class);
            jsonDocMgr.read(summary.getUri(), jacksonHandle);
            Drink drink = jacksonHandle.get();
            System.out.println(drink);
        }

	}
	
	private static void searchRawQueryDocument(String searchCriteria) {
		StringHandle handle = new StringHandle();
		handle.withFormat(Format.JSON).set(searchCriteria);
		RawQueryByExampleDefinition query = queryMgr.newRawQueryByExampleDefinition(handle);
		DocumentPage documents = jsonDocMgr.search(query, 1);
		System.out.println("Total matching documents: " + documents.getTotalSize());
		for (DocumentRecord document : documents) {
			System.out.println(document.getUri());
			String doc = document.getContent(handle).get();
			System.out.println(doc);
		}
	}
	
	private static void writeMultiplePojos(List<Drink> drinks) {
		DocumentWriteSet batch = jsonDocMgr.newWriteSet();
		for (Drink drink : drinks) {
			batch.add(drink.getName(), new JacksonDatabindHandle<Drink>(drink));
		}
		jsonDocMgr.write(batch);

	}

	private static void readMultiplePojos(List<Drink> drinks, String context) {
		List<String> list = drinks.stream().map(d -> d.getName()).collect(Collectors.toList());
		DocumentPage page = jsonDocMgr.read(list.stream().toArray(String[]::new));
		while (page.hasNext()) {
			DocumentRecord document = page.next();
			JacksonDatabindHandle<Drink> handle = new JacksonDatabindHandle<App.Drink>(Drink.class);
			Drink drink = document.getContent(handle).get();
			System.out.println(context + drink.getName());
			System.out.println(drink);			
		}
	}

	/**
	 * asynch write of data to marklogic server - this method will pick up all files
	 * in a folder and write them to the server
	 */
	private static void writeBatchJob() {
		DataMovementManager dmm = client.newDataMovementManager();
		WriteBatcher batcher = dmm.newWriteBatcher();
		batcher.withBatchSize(5).withThreadCount(3).onBatchSuccess(batch -> {
			System.out.println(batch.getTimestamp().getTime() + " documents written: " + batch.getJobWritesSoFar());
		}).onBatchFailure((batch, throwable) -> {
			throwable.printStackTrace();
		});
		// start the job and feed input to the batcher
		dmm.startJob(batcher);
		try {
			Files.walk(Paths.get(DATA_DIR)).filter(Files::isRegularFile).forEach(p -> {
				String uri = "/dmsdk/" + p.getFileName().toString();
				FileHandle handle = new FileHandle().with(p.toFile());
				batcher.add(uri, handle);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Start any partial batches waiting for more input, then wait
		// for all batches to complete. This call will block.
		batcher.flushAndWait();
		dmm.stopJob(batcher);
	}

	private static void readBatchJob(String someQuery) {
		DataMovementManager dmm = client.newDataMovementManager();

		// Construct a directory query with which to drive the job.
		StructuredQueryBuilder sqb = queryMgr.newStructuredQueryBuilder();
		StructuredQueryDefinition query = sqb.directory(true, "/dmsdk/");
		// Create and configure the batcher
		QueryBatcher batcher = dmm.newQueryBatcher(query);
		batcher.onUrisReady(new ExportListener().onDocumentReady(doc -> {
			String uriParts[] = doc.getUri().split("/");
			try {
				Files.write(Paths.get(EX_DIR, "output", uriParts[uriParts.length - 1]),
						doc.getContent(new StringHandle()).toBuffer());
			} catch (Exception e) {
				e.printStackTrace();
			}
		})).onQueryFailure(exception -> exception.printStackTrace());
		dmm.startJob(batcher);
		// Wait for the job to complete, and then stop it.
		batcher.awaitCompletion();
		dmm.stopJob(batcher);

		batcher.awaitCompletion();
	}

	public static void main(String[] args) {

		String uri = "/afternoon-drink";
		
		createDocument(uri, "{name: \"Iced Mocha\", size: \"Grandé\", tasty: true}\n");
		readDocument(uri, "New Document");

		// updating is also done with the write() method
		createDocument(uri, "{name: \"Iced Mocha\", size: \"Grandé\", tasty: true, updated: true, rui: {}}\n");

		// reading the updated document
		readDocument(uri, "Updated Document");

		// patching a document - needs to be an object node
		patchDocument(uri, "/rui", "{whoIsGreat: \"Rui is\"}");

		// reading the patched document
		readDocument(uri, "Patched Document");

		// having marklogic generating URI for a document
		//createDocumentAndURI("{name: \"Chocolate Mocha\", size: \"Grandé\", tasty: true}");
		
		// creating a text document
		//createTextDocument("/example/text.txt", "A simple text document");

		// deleting a document
		//deleteDocument("/afternoon-drink");

		// write multiple docs
		Map<String, String> docsMap = new HashMap<String, String>();
		docsMap.put("/beer", "{ tasty: true, refreshing: true } ");
		docsMap.put("/red-wine", "{ tasty: true, refreshing: false } ");
		docsMap.put("/white-wine", "{ tasty: false, refreshing: true } ");
		createMultipleDocuments(docsMap);
		readMultipleDocuments(docsMap);

		// write multiple POJOS
		List<Drink> drinks = new ArrayList<App.Drink>();
		drinks.add(new Drink(true, true, "manhanthan"));
		drinks.add(new Drink(true, true, "black-russina"));
		drinks.add(new Drink(true, true, "long-island"));
		writeMultiplePojos(drinks);
		readMultiplePojos(drinks, "reading pojo: ");

		// searching a document
		searchDocument("true");

		// TODO
		structedSearchDocument("");

		// write asynch
		//writeBatchJob();
		
		searchRawQueryDocument("{ \"$query\": { \"tasty\": true }}");
		searchRawQueryDocument("{ \"$query\": { \"tasty\": false }}");
		// release the client
		client.release();
	}

	private static class Drink {
		private Boolean tasty;
		private Boolean refreshing;
		private String name;

		private Drink() {
			
		}

		private Drink(Boolean tasty, Boolean refreshing, String name) {
			this.tasty = tasty;
			this.refreshing = refreshing;
			this.name = name;
		}

		public Boolean getTasty() {
			return tasty;
		}

		public Boolean getRefreshing() {
			return refreshing;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "Drink [tasty=" + tasty + ", refreshing=" + refreshing + ", name=" + name + "]";
		}
		
	}
}
