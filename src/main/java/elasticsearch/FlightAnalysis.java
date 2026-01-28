package elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FlightAnalysis {

    // c) Write flight data from airlinesLines.json into Elasticsearch
    public static void writeFlightData(ElasticsearchClient client) throws IOException {

        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        FlightAnalysis.class
                                .getClassLoader()
                                .getResourceAsStream("airlinesLines.json")
                )
        );

        String line;
        while ((line = reader.readLine()) != null) {
            JsonData json = JsonData.fromJson(line);

            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index("airlines")
                            .document(json)
                    )
            );
        }

        BulkResponse response = client.bulk(bulkBuilder.build());

        if (response.errors()) {
            System.err.println("Error while indexing flight data");
        } else {
            System.out.println("Flight data indexed successfully");
        }
    }

    // d) Flights with at least 2500 delays and no security delays
    public static void querySecurityDelay(ElasticsearchClient client) throws IOException {

        SearchResponse<JsonData> response = client.search(s -> s
                        .index("airlines")
                        .query(q -> q.bool(b -> b
                                .must(m -> m
                                        .range(r -> r
                                                .field("arr_del15")
                                                .gte(JsonData.of(2500))
                                        )
                                )
                                .must(m -> m
                                        .term(t -> t
                                                .field("security_ct")
                                                .value(0)
                                        )
                                )
                        )),
                JsonData.class
        );

        System.out.println("Flights with ≥2500 delays and no security delays:");
        for (Hit<JsonData> hit : response.hits().hits()) {
            System.out.println(hit.source().toJson().toString());
        }
    }

    // e) Top 5 airlines by total number of delays
    public static void topDelays(ElasticsearchClient client) throws IOException {

        SearchResponse<Void> response = client.search(s -> s
                        .index("airlines")
                        .size(0)
                        .aggregations("top_airlines", a -> a
                                .terms(t -> t
                                        .field("carrier.keyword")
                                        .size(5)
                                )
                                .aggregations("total_delays", sub -> sub
                                        .sum(sum -> sum
                                                .field("arr_del15")
                                        )
                                )
                        ),
                Void.class
        );

        System.out.println("Top 5 airlines by total delays:");

        for (StringTermsBucket bucket :
                response.aggregations()
                        .get("top_airlines")
                        .sterms()
                        .buckets()
                        .array()) {

            double totalDelays = bucket.aggregations()
                    .get("total_delays")
                    .sum()
                    .value();

            System.out.println(
                    bucket.key().stringValue() + ": " + (long) totalDelays
            );
        }
    }
}
