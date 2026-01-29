package elasticsearch;


public class HelloElastic {
    public static final String INDEX = "team";

    /**
     * Task (a) + (b):
     * - Create TeamMitglied record (first + last name)
     * - Bulk index members into the same index
     * - Query index and print all members using SearchResponse<TeamMitglied>
     *
     * Configure connection:
     * - export ES_API_KEY="..."   (from start-local script output)
     * - optional: export ES_URL="http://localhost:9200"
     */
    public static void main(String[] args) throws Exception {
        try (var client = connect()) {
            var members = java.util.List.of(new TeamMitglied("Jayesh", "Daga"));

            // Ensure old runs don't leak into output (e.g., "Erika Musterfrau").
            if (client.indices().exists(e -> e.index(INDEX)).value()) {
                client.indices().delete(d -> d.index(INDEX));
            }
            client.indices().create(c -> c.index(INDEX));

            writeTeamMembers(client, members);
            queryAndPrintTeamMembers(client);
        }
    }

    public record TeamMitglied(String firstName, String lastName) {
    }

    public static void writeTeamMembers(co.elastic.clients.elasticsearch.ElasticsearchClient client,
                                        java.util.List<TeamMitglied> members) throws java.io.IOException {
        var bulk = new co.elastic.clients.elasticsearch.core.BulkRequest.Builder();

        for (int i = 0; i < members.size(); i++) {
            TeamMitglied m = members.get(i);
            String id = Integer.toString(i + 1);
            bulk.operations(op -> op.index(idx -> idx
                    .index(INDEX)
                    .id(id)
                    .document(m)
            ));
        }

        var response = client.bulk(bulk.build());
        if (response.errors()) {
            System.err.println("Bulk indexing had errors:");
            response.items().stream()
                    .filter(it -> it.error() != null)
                    .forEach(it -> System.err.println(" - id=" + it.id() + " error=" + it.error().reason()));
        } else {
            System.out.println("Indexed " + members.size() + " team members into index '" + INDEX + "'.");
        }

        client.indices().refresh(r -> r.index(INDEX));
    }

    public static void queryAndPrintTeamMembers(co.elastic.clients.elasticsearch.ElasticsearchClient client)
            throws java.io.IOException {
        co.elastic.clients.elasticsearch.core.SearchResponse<TeamMitglied> response =
                client.search(s -> s
                                .index(INDEX)
                                .size(100)
                                .query(q -> q.matchAll(m -> m)),
                        TeamMitglied.class);

        System.out.println("Team members from Elasticsearch:");
        response.hits().hits().forEach(hit -> {
            TeamMitglied m = hit.source();
            if (m != null) {
                System.out.println(" - " + m.firstName() + " " + m.lastName());
            }
        });
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
