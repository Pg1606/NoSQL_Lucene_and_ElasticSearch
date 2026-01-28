package elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;

public class HelloElastic {

    // a) TeamMitglied record
    public record TeamMitglied(String firstName, String lastName) {}

    public static void main(String[] args) throws IOException {

        String apiKey = "PASTE_YOUR_API_KEY_HERE";

        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)
        ).setDefaultHeaders(new BasicHeader[]{
                new BasicHeader("Authorization", "ApiKey " + apiKey)
        }).build();

        ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);

        // a) create and index team members using BulkRequest
        List<TeamMitglied> team = List.of(
                new TeamMitglied("Alice", "Mueller"),
                new TeamMitglied("Bob", "Schmidt"),
                new TeamMitglied("Charlie", "Weber")
        );

        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        for (TeamMitglied member : team) {
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index("teammitglieder")
                            .document(member)
                    )
            );
        }

        BulkResponse bulkResponse = client.bulk(bulkBuilder.build());

        if (bulkResponse.errors()) {
            System.err.println("Error while indexing team members");
        } else {
            System.out.println("Team members indexed successfully");
        }

        // b) query and output all team members
        SearchResponse<TeamMitglied> response = client.search(s -> s
                        .index("teammitglieder")
                        .query(q -> q.matchAll(m -> m)),
                TeamMitglied.class
        );

        System.out.println("Team members:");
        for (Hit<TeamMitglied> hit : response.hits().hits()) {
            TeamMitglied t = hit.source();
            System.out.println(t.firstName() + " " + t.lastName());
        }

        transport.close();
        restClient.close();
    }
}
