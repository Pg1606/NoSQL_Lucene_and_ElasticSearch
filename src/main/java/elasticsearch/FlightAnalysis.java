package elasticsearch;


public class FlightAnalysis {
    public static final String AIRLINES_INDEX = "airlines";
    public static final String RESOURCE_FILE = "airlinesLines.json";

    public static void main(String[] args) throws Exception {
        try (var client = connect()) {
            writeFlightData(client);
            querySecurityDelay(client);
            topDelays(client);
        }
    }

    /**
     * Task (c):
     * Use a BulkRequest and JsonData to add the entries from airlinesLines.json into index "airlines".
     */
    public static void writeFlightData(co.elastic.clients.elasticsearch.ElasticsearchClient client) throws java.io.IOException {
        // Make repeated runs deterministic (avoid duplicates).
        if (client.indices().exists(e -> e.index(AIRLINES_INDEX)).value()) {
            client.indices().delete(d -> d.index(AIRLINES_INDEX));
        }
        client.indices().create(c -> c.index(AIRLINES_INDEX));

        var inputStream = FlightAnalysis.class.getClassLoader().getResourceAsStream(RESOURCE_FILE);
        if (inputStream == null) {
            throw new IllegalStateException("Resource not found: " + RESOURCE_FILE);
        }

        int total = 0;
        int batchSize = 1000;
        var bulk = new co.elastic.clients.elasticsearch.core.BulkRequest.Builder();
        int opsInBatch = 0;

        try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                // The assignment explicitly asks to use JsonData.
                // We keep the original JSON structure and let Elasticsearch infer mappings.
                co.elastic.clients.json.JsonData json = co.elastic.clients.json.JsonData.fromJson(line);
                bulk.operations(op -> op.index(idx -> idx.index(AIRLINES_INDEX).document(json)));
                opsInBatch++;

                total++;
                if (total % batchSize == 0) {
                    executeBulk(client, bulk, total);
                    bulk = new co.elastic.clients.elasticsearch.core.BulkRequest.Builder();
                    opsInBatch = 0;
                }
            }
        }

        if (opsInBatch > 0) {
            executeBulk(client, bulk, total);
        }

        client.indices().refresh(r -> r.index(AIRLINES_INDEX));
        System.out.println("Finished writing flight data. Documents processed: " + total);
    }

    private static void executeBulk(co.elastic.clients.elasticsearch.ElasticsearchClient client,
                                    co.elastic.clients.elasticsearch.core.BulkRequest.Builder bulk,
                                    int totalSoFar) throws java.io.IOException {
        var response = client.bulk(bulk.build());
        if (response.errors()) {
            System.err.println("Bulk indexing reported errors (after " + totalSoFar + " docs). Showing first few:");
            response.items().stream()
                    .filter(it -> it.error() != null)
                    .limit(10)
                    .forEach(it -> System.err.println(" - id=" + it.id() + " error=" + it.error().reason()));
        } else {
            System.out.println("Bulk indexed up to " + totalSoFar + " docs...");
        }
    }

    /**
     * Task (d):
     * Search for entries where at least 2500 flights were delayed,
     * but none of the delays were caused by security issues (security delay count == 0).
     */
    public static void querySecurityDelay(co.elastic.clients.elasticsearch.ElasticsearchClient client) throws java.io.IOException {
        String delayedField = "statistics.flights.delayed";
        String securityDelayCountField = "statistics.# of delays.security";

        var response = client.search(s -> s
                        .index(AIRLINES_INDEX)
                        .size(25)
                        .query(q -> q.bool(b -> b
                                .filter(f -> f.range(r -> r
                                        .number(n -> n
                                                .field(delayedField)
                                                .gte(2500d)
                                        )
                                ))
                                .filter(f -> f.term(t -> t
                                        .field(securityDelayCountField)
                                        .value(v -> v.longValue(0))
                                ))
                        )),
                co.elastic.clients.json.JsonData.class);

        System.out.println("Query (>=2500 delayed flights AND 0 security delays) results: " + response.hits().hits().size());
        response.hits().hits().forEach(hit -> {
            var src = hit.source();
            if (src != null) {
                System.out.println(src.toString());
            }
        });
    }

    /**
     * Task (e):
     * Find the top 5 airlines with the highest total number of delays across all airports using an aggregation query.
     *
     * We interpret "total number of delays" as total delayed flights: statistics.flights.delayed
     */
    public static void topDelays(co.elastic.clients.elasticsearch.ElasticsearchClient client) throws java.io.IOException {
        String airlineField = "carrier.name.keyword";
        String delayedField = "statistics.flights.delayed";

        co.elastic.clients.elasticsearch.core.SearchResponse<Void> response;
        try {
            response = topDelaysAgg(client, airlineField, delayedField);
        } catch (co.elastic.clients.elasticsearch._types.ElasticsearchException e) {
            // Some dynamic mappings might not create the ".keyword" subfield. Fall back to plain field.
            airlineField = "carrier.name";
            response = topDelaysAgg(client, airlineField, delayedField);
        }

        var agg = response.aggregations().get("top_airlines");
        if (agg == null || agg.sterms() == null) {
            System.out.println("No aggregation results (is the index populated and is '" + airlineField + "' mapped as keyword?)");
            return;
        }

        System.out.println("Top 5 airlines by total delayed flights:");
        for (var bucket : agg.sterms().buckets().array()) {
            Double sum = bucket.aggregations().get("total_delayed").sum().value();
            long totalDelayed = sum == null ? 0L : Math.round(sum);
            System.out.println(" - " + bucket.key().stringValue() + " -> " + totalDelayed);
        }
    }

    private static co.elastic.clients.elasticsearch.core.SearchResponse<Void> topDelaysAgg(
            co.elastic.clients.elasticsearch.ElasticsearchClient client,
            String airlineField,
            String delayedField
    ) throws java.io.IOException {
        return client.search(s -> s
                        .index(AIRLINES_INDEX)
                        .size(0)
                        .aggregations("top_airlines", a -> a
                                .terms(t -> t
                                        .field(airlineField)
                                        .size(5)
                                        .order(co.elastic.clients.util.NamedValue.of(
                                                "total_delayed",
                                                co.elastic.clients.elasticsearch._types.SortOrder.Desc
                                        ))
                                )
                                .aggregations("total_delayed", sub -> sub
                                        .sum(sm -> sm.field(delayedField))
                                )
                        ),
                Void.class);
    }

    private static co.elastic.clients.elasticsearch.ElasticsearchClient connect() {
        String url = getenvOrPropertyOrDefault("ES_URL", "es.url", "http://localhost:9200");
        String apiKey = requireEnvOrProperty("ES_API_KEY", "es.apiKey");

        // Connection style recommended by Elastic docs:
        // https://www.elastic.co/docs/reference/elasticsearch/clients/java/getting-started#connecting
        return co.elastic.clients.elasticsearch.ElasticsearchClient.of(b -> b
                .host(url)
                .apiKey(apiKey)
        );
    }

    private static String requireEnvOrProperty(String envKey, String propKey) {
        String v = getenvOrPropertyOrDefault(envKey, propKey, null);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(
                    "Missing Elasticsearch API key. Set environment variable " + envKey +
                            " or JVM property -D" + propKey + "=..."
            );
        }
        return v.trim();
    }

    private static String getenvOrPropertyOrDefault(String envKey, String propKey, String defaultValue) {
        String env = System.getenv(envKey);
        if (env != null && !env.isBlank()) return env.trim();
        String prop = System.getProperty(propKey);
        if (prop != null && !prop.isBlank()) return prop.trim();
        return defaultValue;
    }
}
