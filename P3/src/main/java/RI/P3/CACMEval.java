package RI.P3;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
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

	private static class queryRelevance {

		private final int queryId;
		private final List<String> docs;

		public queryRelevance(int queryId, List<String> docs) {

			this.queryId = queryId;
			this.docs = docs;
		}

		public int getQueryId() {
			return queryId;
		}

		public List<String> getDocs() {
			return docs;
		}

	}

	private static class docElement implements Comparable<docElement> {

		private final float doc_score;
		private final String i_docID;
		private final String t_title;
		private final String w_abstract;
		private final String b_date;
		private final String a_authors;
		private final String k_keywords;
		private final String n_entrydate;

		public docElement(float doc_score, String i_docID, String t_title, String w_abstract, String b_date,
				String a_authors, String k_keywords, String n_entrydate) {
			this.doc_score = doc_score;
			this.i_docID = i_docID;
			this.t_title = t_title;
			this.w_abstract = w_abstract;
			this.b_date = b_date;
			this.a_authors = a_authors;
			this.k_keywords = k_keywords;
			this.n_entrydate = n_entrydate;
		}

		public int compareTo(docElement o) {
			return Double.compare(o.doc_score, this.doc_score);
		}

		public float getDoc_score() {
			return doc_score;
		}

		public String getI_docID() {
			return i_docID;
		}

		public String getT_title() {
			return t_title;
		}

		public String getW_abstract() {
			return w_abstract;
		}

		public String getB_date() {
			return b_date;
		}

		public String getA_authors() {
			return a_authors;
		}

		public String getK_keywords() {
			return k_keywords;
		}

		public String getN_entrydate() {
			return n_entrydate;
		}

		public void showFields(List<String> fields) {

			if (fields.contains("docid"))
				System.out.println("	docid: " + this.getI_docID());

			if (fields.contains("title"))
				System.out.println("	title: " + this.getT_title());

			if (fields.contains("abstract"))
				System.out.println("	abstract: " + this.getW_abstract());

			if (fields.contains("date"))
				System.out.println("	date: " + this.getB_date());

			if (fields.contains("authors"))
				System.out.println("	authors: " + this.getA_authors());

			if (fields.contains("keywords"))
				System.out.println("	keywords: " + this.getK_keywords());

			if (fields.contains("entrydate"))
				System.out.println("	entrydate: " + this.getN_entrydate());

			System.out.println("	score: " + this.getDoc_score());

			System.out.println();
		}

	}

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

		private static String extract(String elt, String text, boolean allowEmpty) {

			String startElt = "." + elt;
			int startEltIndex = text.indexOf(startElt);
			if (startEltIndex < 0) {
				if (allowEmpty)
					return "";
				throw new IllegalArgumentException("no start, elt=" + elt + " text=" + text);
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
					minQuery = Integer.parseInt(queryNumbers.substring(0, queryNumbers.indexOf('-')));
					maxQuery = Integer
							.parseInt(queryNumbers.substring(queryNumbers.indexOf('-') + 1, queryNumbers.length()));
				}

				else {
					minQuery = Integer.parseInt(args[i]);
					maxQuery = Integer.parseInt(args[i]);
				}

			}

			else if ("-fieldsproc".equals(args[i])) {

				while (i < args.length && !args[i + 1].startsWith("-")) {

					if (args[i + 1].toLowerCase().equals("w"))
						args[i + 1] = "abstract";

					else if (args[i + 1].toLowerCase().equals("t"))
						args[i + 1] = "title";

					else if (args[i + 1].toLowerCase().equals("k"))
						args[i + 1] = "keywords";

					fieldsProc.add(args[i + 1]);
					i++;
				}
			}

			else if ("-fieldsvisual".equals(args[i])) {

				while (i < args.length && !args[i + 1].startsWith("-")) {

					String field = null;

					if (args[i].toLowerCase().equals("i"))
						field = "docid";
					if (args[i].toLowerCase().equals("t"))
						field = "title";
					if (args[i].toLowerCase().equals("w"))
						field = "abstract";
					if (args[i].toLowerCase().equals("b"))
						field = "date";
					if (args[i].toLowerCase().equals("a"))
						field = "authors";
					if (args[i].toLowerCase().equals("k"))
						field = "keywords";
					if (args[i].toLowerCase().equals("n"))
						field = "entrydate";

					fieldsVisual.add(field);
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

		String relevanceDir = System.getProperty("user.dir");
		relevanceDir = relevanceDir + "\\index\\qrels.text";

		List<queryRelevance> relevances = getRelevances(Paths.get(relevanceDir));

		queryMap = getQueries(Paths.get(queryDir), minQuery, maxQuery);
		doSearch(indexPath, queryMap, topLimit, fieldsProc, fieldsVisual, relevances, cutLimit);
	}

	public static List<queryRelevance> getRelevances(Path file) {

		List<queryRelevance> relevances = new ArrayList<queryRelevance>();

		try {
			byte[] encoded = Files.readAllBytes(file);
			String text = new String(encoded, StandardCharsets.UTF_8);
			String[] lines = text.split("\n");
			int queriesNumber = Integer.parseInt(lines[lines.length - 1].substring(0, 2));
			int index = 0;

			for (int i = 0; i <= queriesNumber; i++) {

				List<String> rls = new ArrayList<String>();

				while (index < lines.length && i == Integer.parseInt((lines[index].substring(0, 2)))) {
					rls.add(lines[index].substring(3, 7));
					index++;
				}

				queryRelevance qr = new queryRelevance(i, rls);
				relevances.add(qr);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return relevances;
	}

	public static Map<Integer, QueryData> getQueries(Path queryFile, int minQuery, int maxQuery) {
		Map<Integer, QueryData> map = new TreeMap<Integer, QueryData>();

		try {

			byte[] encoded = Files.readAllBytes(queryFile);
			String text = new String(encoded, StandardCharsets.UTF_8);

			char[] content = text.toCharArray();
			StringBuffer buffer = new StringBuffer();
			buffer.append(content);

			List<List<String>> dataList = QueryParser.parseString(buffer);
			for (List<String> qData : dataList) {

				Integer queryId = Integer.parseInt(qData.get(0));
				if (minQuery == -1 || (queryId >= minQuery && queryId <= maxQuery)) {

					String query = qData.get(1);
					String authors = qData.get(2);
					String entry = qData.get(3);
					map.put(queryId, new QueryData(query, authors, entry));
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return map;
	}

	public static float processQuery(String indexPath, String query, List<String> procFields, List<String> showFields,
			int top, queryRelevance qr, int cut) {

		IndexReader reader;
		List<docElement> docs = new ArrayList<docElement>();
		float sumP = 0;

		try {

			reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);

			String[] arrayQuery = new String[procFields.size()];
			String[] arrayField = new String[procFields.size()];
			Occur[] arrayClauses = new Occur[procFields.size()];

			query = QueryParserUtil.escape(query);

			for (int i = 0; i < procFields.size(); i++) {

				arrayQuery[i] = query;
				arrayField[i] = procFields.get(i);
				arrayClauses[i] = BooleanClause.Occur.SHOULD;
			}

			BooleanQuery booleanQuery = (BooleanQuery) MultiFieldQueryParser.parse(arrayQuery, arrayField, arrayClauses,
					analyzer);

			TotalHitCountCollector collector = new TotalHitCountCollector();
			searcher.search(booleanQuery, collector);
			TopDocs results = searcher.search(booleanQuery, collector.getTotalHits());
			ScoreDoc[] hits = results.scoreDocs;

			for (int i = 0; i < collector.getTotalHits(); i++) {
				Document doc = searcher.doc(hits[i].doc);
				Float score = hits[i].score;
				docElement de = new docElement(score, doc.get("docid"), doc.get("title"), doc.get("abstract"),
						doc.get("date"), doc.get("authors"), doc.get("keywords"), doc.get("entrydate"));

				docs.add(de);
			}

			Collections.sort(docs);
			System.out.println("P@10: " + calculatePK(10, qr, docs));
			System.out.println("P@20: " + calculatePK(20, qr, docs));
			System.out.println("Recall@10: " + calculateRK(10, qr, docs));
			System.out.println("Recall@20: " + calculateRK(20, qr, docs));

			System.out.println();

			for (int i = 0; i < cut; i++) {
				sumP += calculatePK(i + 1, qr, docs);
			}

			for (int i = 0; i < top; i++) {
				docElement de = docs.get(i);

				if (qr != null && qr.getDocs().contains(de.getI_docID())) {
					System.out.println("	***THE FOLLOWING DOC IS RELEVANT***");
					System.out.println("	Top " + (i + 1) + " doc");
				}

				else
					System.out.println("	Top " + (i + 1) + " doc");

				System.out.println("	Avg. Precision: " + calculatePK(i + 1, qr, docs));

				de.showFields(showFields);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return sumP / (float) cut;
	}

	private static float calculateRK(int k, queryRelevance qr, List<docElement> docs) {

		int relevants = 0;

		for (int i = 0; i < k; i++) {

			if (qr.getDocs().contains(docs.get(i).getI_docID()))
				relevants++;
		}

		float pk = (relevants / (float) qr.getDocs().size());
		return pk;

	}

	private static float calculatePK(float k, queryRelevance qr, List<docElement> docs) {

		int relevants = 0;

		for (int i = 0; i < k; i++) {

			if (qr.getDocs().contains(docs.get(i).getI_docID()))
				relevants++;
		}
		float rk = (relevants / k);
		return rk;
	}

	public static void doSearch(String indexPath, Map<Integer, QueryData> mapQueries, int top, List<String> procFields,
			List<String> showFields, List<queryRelevance> relevances, int cut) {

		float map = 0;

		System.err.println("Showing requested search info...\n");

		for (Map.Entry<Integer, QueryData> entry : mapQueries.entrySet()) {
			int queryNumber = entry.getKey();
			String query = entry.getValue().getQuery();
			queryRelevance qr = null;

			System.out.println("QueryID :" + queryNumber);
			System.out.println("Query Body :" + query);
			System.out.println("Showing top " + top + " docs:");

			if (queryNumber <= (relevances.size() + 1))
				qr = relevances.get(queryNumber);

			map += processQuery(indexPath, query, procFields, showFields, top, qr, cut);
			System.out.println();

		}

		System.err.println("Mean average precision: " + map / mapQueries.size());

	}

}
