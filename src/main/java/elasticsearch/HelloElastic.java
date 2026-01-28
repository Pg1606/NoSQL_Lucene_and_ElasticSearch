package elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
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

        List<TeamMitglied> team = List.of(
                new TeamMitglied("Alice", "Mueller"),
                new TeamMitglied("Bob", "Schmidt")
        );

        BulkRequest.Builder bulk = new BulkRequest.Builder();
        for (TeamMitglied t : team) {
            bulk.operations(op -> op
                    .index(i -> i.index("teammitglieder").document(t))
            );
        }
        client.bulk(bulk.build());

        SearchResponse<TeamMitglied> response = client.search(s -> s
                        .index("teammitglieder")
                        .query(q -> q.matchAll(m -> m)),
                TeamMitglied.class
        );

        for (Hit<TeamMitglied> hit : response.hits().hits()) {
            System.out.println(hit.source());
        }

        transport.close();
        restClient.close();
    }
}
