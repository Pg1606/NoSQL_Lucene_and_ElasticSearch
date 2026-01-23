package lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.ByteBuffersDirectory;

import java.nio.file.Files;
import java.nio.file.Path;

public class HelloLucene {

    public static void main(String[] args) throws Exception {

        // 1. Analyzer
        Analyzer analyzer = new StandardAnalyzer();

        // 2. In-memory index directory
        Directory directory = new ByteBuffersDirectory();

        // 3. Index writer configuration
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // 4. Create documents
        addDoc(writer, "My First Lucene Program");
        addDoc(writer, "I Love Lucene");
        addDoc(writer, "Why?");
        addDoc(writer, "I Hate Lucene");

        // 5. Close writer (commit changes)
        writer.close();

        // 6. Read access to index
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // 7. QueryParser on field "title"
        QueryParser parser = new QueryParser("title", analyzer);

        // This query matches exactly two documents
        Query query = parser.parse("\"First Lucene\" OR \"Love Lucene\"");

        // 8. Execute search
        TopDocs results = searcher.search(query, 2);

        // 9. Output results
        System.out.println("--- (a) Hello Lucene ---");
        System.out.println("Total hits: " + results.totalHits);

        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = reader.storedFields().document(scoreDoc.doc);
            System.out.println(" - " + doc.get("title"));
        }

        reader.close();
        directory.close();


        // -----------------------------
        // 1. Create temporary index path
        // -----------------------------
        Path indexPath = Files.createTempDirectory("german_lit_index");
        System.out.println("Index directory: " + indexPath.toAbsolutePath());

        // -----------------------------
        // 2. Write documents to index
        // -----------------------------
        System.out.println("\n--- (b) Writing documents to index ---");
        Path textsPath = Path.of("src/main/resources");
        GermanLiterature.writeToIndex(indexPath, textsPath);

        // -----------------------------
        // 3. Run hasFaustQuery
        // -----------------------------
        System.out.println("\n--- (c) hasFaustQuery ---");
        GermanLiterature.hasFaustQuery(indexPath);

        // -----------------------------
        // 4. Run moreThanFaustQuery
        // -----------------------------
        System.out.println("\n--- (d) moreThanFaustQuery ---");
        GermanLiterature.moreThanFaustQuery(indexPath);

        // -----------------------------
        // 5. Run frequentTerms
        // -----------------------------
        System.out.println("\n--- (e) frequentTerms ---");
        GermanLiterature.frequentTerms(indexPath);

        // -----------------------------
        // 6. Run similiarDocuments
        // -----------------------------
        System.out.println("\n--- (f) similiarDocuments ---");
        GermanLiterature.similiarDocuments(indexPath);

        // -----------------------------
        // 7. Run frequentTermsWithStopWords
        // -----------------------------
        Path stopwordFreeIndex = Files.createTempDirectory("german_lit_index_no_stopwords");
        System.out.println("\nIndex without stopwords: " + stopwordFreeIndex.toAbsolutePath());
        System.out.println("\n--- (g) frequentTermsWithStopWords ---");
        GermanLiterature.frequentTermsWithStopWords(indexPath, stopwordFreeIndex);

        System.out.println("\n--- All tasks completed ---");
    }

    private static void addDoc(IndexWriter writer, String title) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("title", title, Field.Store.YES));
        writer.addDocument(doc);
    }
}