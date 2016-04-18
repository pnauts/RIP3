package RI.P3;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class CACMEval {

	public static class QueryData {

		private final String query;
		private final String authors;
		private final String entry;

		public QueryData(String query, String authors, String entry) {

			this.query = query;
			this.authors = authors;
			this.entry = entry;
		}

		public String getQuery() {
			return query;
		}

		public String getAuthors() {
			return authors;
		}

		public String getEntry() {
			return entry;
		}

	}

	public static class QueryParser {

		public static List<List<String>> parseString(StringBuffer fileContent) {

			String text = fileContent.toString();
			String[] lines = text.split("\n");
			List<List<String>> documents = new LinkedList<List<String>>();

			for (int i = 0; i < lines.length; ++i) {
				if (!lines[i].startsWith(".I"))
					continue;
				StringBuilder sb = new StringBuilder();
				sb.append(lines[i++]);
				sb.append("\n");
				while (i < lines.length && !lines[i].startsWith(".I")) {
					sb.append(lines[i++]);
					sb.append("\n");
				}
				i--;
				documents.add(handleDocument(sb.toString()));
			}
			return documents;
		}

		public static List<String> handleDocument(String text) {

			String queryid = extract("I", text, true);
			String query = extract("W\n", text, true).replaceAll("\n", " ");
			String authors = extract("A\n", text, true).replaceAll("\n", " ");
			String entry = extract("N\n", text, true).replaceAll("\n", " ");

			List<String> document = new LinkedList<String>();
			document.add(queryid.replace(" ", ""));
			document.add(query);
			document.add(authors);
			document.add(entry);
			return document;
		}

		private static String extract(String elt, String text,
				boolean allowEmpty) {

			String startElt = "." + elt;
			int startEltIndex = text.indexOf(startElt);
			if (startEltIndex < 0) {
				if (allowEmpty)
					return "";
				throw new IllegalArgumentException("no start, elt=" + elt
						+ " text=" + text);
			}
			int start = startEltIndex + startElt.length();
			String endElt = "\n.";
			int end = text.indexOf(endElt, start);
			if (end < 0)
				return text.substring(start, text.length() - 1);
			return text.substring(start, end);
		}
	}

	public static void main(String[] args) {

		String indexPath = null;
		int cutLimit = -1, topLimit = -1;
		int minQuery = -1, maxQuery = -1;
		List<String> fieldsProc = new ArrayList<String>();
		List<String> fieldsVisual = new ArrayList<String>();
		int tq = -1, td = -1, ndr = -1, evMode = -1;
		Map<Integer, QueryData> queryMap = new HashMap<Integer, QueryData>();

		for (int i = 0; i < args.length; i++) {

			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			}

			else if ("-cut".equals(args[i])) {
				cutLimit = Integer.parseInt((args[i + 1]));
				i++;
			}

			else if ("-top".equals(args[i])) {
				topLimit = Integer.parseInt((args[i + 1]));
				i++;
			}

			else if ("-queries".equals(args[i])) {
				i++;

				if ("all".equals(args[i]))
					continue;

				if (args[i].indexOf('-') != -1) {
					String queryNumbers = args[i].replace(" ", "");
					minQuery = Integer.parseInt(queryNumbers.substring(0,
							queryNumbers.indexOf('-')));
					maxQuery = Integer.parseInt(queryNumbers.substring(
							queryNumbers.indexOf('-') + 1,
							queryNumbers.length() - 1));
				}

				else
					minQuery = Integer.parseInt(args[i]);

			}

			else if ("-fieldsproc".equals(args[i])) {
				i++;
				while (i < args.length && !args[i].startsWith("-")) {
					
					if(args[i].toLowerCase().equals("w"))
						args[i] = "abstract";
					
					else if(args[i].toLowerCase().equals("t"))
						args[i] = "title";
					
					else if(args[i].toLowerCase().equals("k"))
						args[i] = "keywords";
					
					fieldsProc.add(args[i]);
					i++;
				}
			}

			else if ("-fieldsvisual".equals(args[i])) {
				i++;
				while (i < args.length && !args[i].startsWith("-")) {
					fieldsVisual.add(args[i]);
					i++;
				}
			}

			else if ("-rf1".equals(args[i])) {
				evMode = 1;
				tq = Integer.parseInt(args[i + 1]);
				td = Integer.parseInt(args[i + 2]);
				ndr = Integer.parseInt(args[i + 3]);
				i += 3;
			}

			else if ("-rf2".equals(args[i])) {
				evMode = 2;
				ndr = Integer.parseInt(args[i + 1]);
				i++;
			}

			else if ("-rf3".equals(args[i])) {
				evMode = 3;
				ndr = Integer.parseInt(args[i + 1]);
				i++;
			}
		}

		String queryDir = System.getProperty("user.dir");
		queryDir = queryDir + "\\index\\query.text";
		queryMap = getQueries(Paths.get(queryDir));
		
		processQuery(indexPath, "What articles exist which deal with TSS (Time Sharing System), an operating system for IBM computers?",fieldsProc);
	}

	public static Map<Integer, QueryData> getQueries(Path queryFile) {
		Map<Integer, QueryData> map = new HashMap<Integer, QueryData>();

		try {

			byte[] encoded = Files.readAllBytes(queryFile);
			String text = new String(encoded, StandardCharsets.UTF_8);

			char[] content = text.toCharArray();
			StringBuffer buffer = new StringBuffer();
			buffer.append(content);

			List<List<String>> dataList = QueryParser.parseString(buffer);
			for (List<String> qData : dataList) {

				Integer queryId = Integer.parseInt(qData.get(0));
				String query = qData.get(1);
				String authors = qData.get(2);
				String entry = qData.get(3);

				map.put(queryId, new QueryData(query, authors, entry));
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return map;
	}

	public static void processQuery(String indexPath, String query,
			List<String> fields) {

		IndexReader reader;

		try {

			reader = DirectoryReader
					.open(FSDirectory.open(new File(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);

			String[] arrayQuery = new String[fields.size()];
			String[] arrayField = new String[fields.size()];
			Occur[] arrayClauses = new Occur[fields.size()];

			for (int i = 0; i < fields.size(); i++) {

				arrayQuery[i] = query;
				arrayField[i] = fields.get(i);
				arrayClauses[i] = BooleanClause.Occur.SHOULD;
			}

			MultiFieldQueryParser parser = new MultiFieldQueryParser(
					Version.LUCENE_40, arrayField, analyzer);

			BooleanQuery booleanQuery = (BooleanQuery) MultiFieldQueryParser
					.parse(arrayQuery, arrayField, arrayClauses, analyzer);

			TotalHitCountCollector collector = new TotalHitCountCollector();
			searcher.search(booleanQuery, collector);
			TopDocs results = searcher.search(booleanQuery,
					collector.getTotalHits());
			ScoreDoc[] hits = results.scoreDocs;
			
			for (int i = 0; i < results.totalHits; i++) {
				Document doc = searcher.doc(hits[i].doc);

				System.out.println("DocID: " + doc.get("docid"));
				System.out.println("Title: " + doc.get("title"));
				System.out.println("Score: " + hits[i].score);
				System.out.println();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
