package lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.search.*;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.analysis.CharArraySet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class GermanLiterature {

    public static void writeToIndex(Path indexPath, Path textsPath) throws IOException {

        // 1. German analyzer
        Analyzer analyzer = new GermanAnalyzer();

        // 2. Persistent index directory
        Directory directory = FSDirectory.open(indexPath);

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // 3. FieldType for content WITH term vectors
        FieldType contentType = new FieldType();
        contentType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        contentType.setTokenized(true);
        contentType.setStored(true);
        contentType.setStoreTermVectors(true);
        contentType.freeze();

        // 4. Add all documents
        addDocument(writer, "Die Räuber", "Friedrich Schiller",
                textsPath.resolve("Die_Raeuber.txt"), contentType);

        addDocument(writer, "Effi Briest", "Theodor Fontane",
                textsPath.resolve("Effi_Briest.txt"), contentType);

        addDocument(writer, "Egmont", "Johann Wolfgang von Goethe",
                textsPath.resolve("Egmont.txt"), contentType);

        addDocument(writer, "Faust I", "Johann Wolfgang von Goethe",
                textsPath.resolve("Faust_I.txt"), contentType);

        addDocument(writer, "Faust II", "Johann Wolfgang von Goethe",
                textsPath.resolve("Faust_II.txt"), contentType);

        addDocument(writer, "Die Verwandlung", "Franz Kafka",
                textsPath.resolve("Die_Verwandlung.txt"), contentType);

        writer.close();
        directory.close();
    }

    private static void addDocument(IndexWriter writer,
                                    String title,
                                    String author,
                                    Path filePath,
                                    FieldType contentType) throws IOException {

        String content = Files.readString(filePath);

        Document doc = new Document();
        doc.add(new StringField("title", title, Field.Store.YES));
        doc.add(new StringField("author", author, Field.Store.YES));
        doc.add(new Field("content", content, contentType));

        writer.addDocument(doc);
    }

    public static void hasFaustQuery(Path indexPath) throws IOException {

        // 1. Open index
        Directory directory = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // 2. TermQuery for "faust" in content field
        TermQuery query = new TermQuery(new Term("content", "faust"));

        // 3. Execute search
        TopDocs results = searcher.search(query, 10);

        // 4. Output titles
        System.out.println("Documents containing 'faust':");

        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = reader.storedFields().document(scoreDoc.doc);
            System.out.println(" - " + doc.get("title"));
        }

        reader.close();
        directory.close();
    }

    public static void moreThanFaustQuery(Path indexPath) throws IOException {

        Directory directory = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        /* --------------------------------------------------
         * 1. BooleanQuery: faust AND mephistophel
         * -------------------------------------------------- */

        TermQuery faustQuery = new TermQuery(new Term("content", "faust"));
        TermQuery mephQuery = new TermQuery(new Term("content", "mephistophel"));

        BooleanQuery booleanQuery = new BooleanQuery.Builder()
                .add(faustQuery, BooleanClause.Occur.MUST)
                .add(mephQuery, BooleanClause.Occur.MUST)
                .build();

        TopDocs booleanResults = searcher.search(booleanQuery, 10);

        System.out.println("BooleanQuery results:");
        for (ScoreDoc sd : booleanResults.scoreDocs) {
            Document doc = reader.storedFields().document(sd.doc);
            System.out.println(" - " + doc.get("title"));
        }

        /* --------------------------------------------------
         * 2. PhraseQuery with slop
         * -------------------------------------------------- */

        PhraseQuery phraseQuery = new PhraseQuery.Builder()
                .add(new Term("content", "faust"))
                .add(new Term("content", "mephistophel"))
                .setSlop(50)   // large enough to bridge distance
                .build();

        TopDocs phraseResults = searcher.search(phraseQuery, 10);

        System.out.println("\nPhraseQuery results:");
        for (ScoreDoc sd : phraseResults.scoreDocs) {
            Document doc = reader.storedFields().document(sd.doc);
            System.out.println(" - " + doc.get("title"));
        }

        reader.close();
        directory.close();
    }

    public static void frequentTerms(Path indexPath) throws Exception {

        // 1. Open index
        Directory directory = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(directory);

        // 2. Get top 20 most frequent terms in "content"
        TermStats[] topTerms = HighFreqTerms.getHighFreqTerms(
                reader,
                20,
                "content",
                new HighFreqTerms.TotalTermFreqComparator()
        );

        // 3. Output results
        System.out.println("Top 20 most frequent terms:");

        for (TermStats stat : topTerms) {
            System.out.println(stat.termtext.utf8ToString()
                    + " (" + stat.totalTermFreq + ")");
        }

        reader.close();
        directory.close();
    }

    public static void similiarDocuments(Path indexPath) throws Exception {

        // 1. Open index
        Directory directory = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Analyzer analyzer = new GermanAnalyzer();

        // 2. MoreLikeThis from lucene.misc
        MoreLikeThis mlt = new MoreLikeThis(reader);
        mlt.setAnalyzer(analyzer);
        mlt.setFieldNames(new String[]{"content"});
        mlt.setMinTermFreq(5);
        mlt.setMinDocFreq(2);

        // 3. Find Faust I doc ID
        int faustDocId = -1;
        for (int i = 0; i < reader.maxDoc(); i++) {
            Document doc = reader.storedFields().document(i);
            if ("Faust I".equals(doc.get("title"))) {
                faustDocId = i;
                break;
            }
        }
        if (faustDocId == -1) {
            System.out.println("Faust I not found in index.");
            reader.close();
            directory.close();
            return;
        }

        // 4. Create MoreLikeThis query
        Query query = mlt.like(faustDocId);

        // 5. Execute search
        TopDocs results = searcher.search(query, 10);

        System.out.println("Documents similar to Faust I:");
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = reader.storedFields().document(sd.doc);
            System.out.printf(" - %s (score: %.4f)%n",
                    doc.get("title"), sd.score);
        }

        reader.close();
        directory.close();
    }

    public static void frequentTermsWithStopWords(Path originalIndexPath, Path newIndexPath) throws Exception {

        // 1. Create a new index directory
        Directory directory = FSDirectory.open(newIndexPath);

        // 2. GermanAnalyzer without stop words
        Analyzer analyzer = new GermanAnalyzer(CharArraySet.EMPTY_SET);

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        // 3. FieldType for content with term vectors
        FieldType contentType = new FieldType();
        contentType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        contentType.setTokenized(true);
        contentType.setStored(true);
        contentType.setStoreTermVectors(true);
        contentType.freeze();

        // 4. Re-index all documents from original index
        Directory originalDirectory = FSDirectory.open(originalIndexPath);
        try (IndexReader reader = DirectoryReader.open(originalDirectory)) {
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document oldDoc = reader.storedFields().document(i);

                String title = oldDoc.get("title");
                String author = oldDoc.get("author");
                String content = oldDoc.get("content");

                Document newDoc = new Document();
                newDoc.add(new StringField("title", title, Field.Store.YES));
                newDoc.add(new StringField("author", author, Field.Store.YES));
                newDoc.add(new Field("content", content, contentType));

                writer.addDocument(newDoc);
            }
        }

        writer.close();
        originalDirectory.close();

        // 5. Open the new index and run HighFreqTerms
        DirectoryReader newReader = DirectoryReader.open(directory);
        TermStats[] topTerms = HighFreqTerms.getHighFreqTerms(
                newReader,
                20,
                "content",
                new HighFreqTerms.TotalTermFreqComparator()
        );

        System.out.println("Top 20 most frequent terms WITHOUT stop words:");
        Arrays.stream(topTerms).forEach(stat ->
                System.out.println(stat.termtext.utf8ToString() + " (" + stat.totalTermFreq + ")")
        );

        newReader.close();
        directory.close();
    }
}