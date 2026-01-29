package elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import java.io.IOException;
import java.util.List;

public class HelloElastic {

    // a) TeamMitglied record
    public record TeamMitglied(String firstName, String lastName) {}

    public static void main(String[] args) throws IOException {

        // The client is provided by the template infrastructure
        // (connection details are handled outside this class)
        ElasticsearchClient client = ElasticsearchConnection.client();

        List<TeamMitglied> team = List.of(
                new TeamMitglied("Alice", "Mueller"),
                new TeamMitglied("Bob", "Schmidt"),
                new TeamMitglied("Charlie", "Weber")
        );

        // a) Bulk insert
        BulkRequest.Builder bulk = new BulkRequest.Builder();
        for (TeamMitglied t : team) {
            bulk.operations(op -> op
                    .index(idx -> idx
                            .index("teammitglieder")
                            .document(t)
                    )
            );
        }
        client.bulk(bulk.build());

        // b) Query all team members
        SearchResponse<TeamMitglied> response =
                client.search(s -> s
                                .index("teammitglieder")
                                .query(q -> q.matchAll(m -> m)),
                        TeamMitglied.class
                );

        System.out.println("Team members:");
        for (Hit<TeamMitglied> hit : response.hits().hits()) {
            System.out.println(hit.source());
        }
    }
}
