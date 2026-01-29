package elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FlightAnalysis {

    // c) Write airline data
    public static void writeFlightData(ElasticsearchClient client) throws IOException {

        BulkRequest.Builder bulk = new BulkRequest.Builder();

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
            bulk.operations(op -> op
                    .index(idx -> idx
                            .index("airlines")
                            .document(json)
                    )
            );
        }

        client.bulk(bulk.build());
    }

    // d) ≥2500 delays, no security delays
    public static void querySecurityDelay(ElasticsearchClient client) throws IOException {

        SearchResponse<JsonData> response =
                client.search(s -> s
                                .index("airlines")
                                .query(q -> q.bool(b -> b
                                        .must(m -> m.range(r -> r
                                                .field(f -> f
                                                        .name("arr_del15")
                                                        .gte(JsonData.of(2500))
                                                )
                                        ))
                                        .must(m -> m.term(t -> t
                                                .field("security_ct")
                                                .value(0)
                                        ))
                                )),
                        JsonData.class
                );

        for (Hit<JsonData> hit : response.hits().hits()) {
            System.out.println(hit.source().toJson());
        }
    }

    // e) Top 5 airlines by total delays
    public static void topDelays(ElasticsearchClient client) throws IOException {

        SearchResponse<Void> response =
                client.search(s -> s
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

        for (StringTermsBucket bucket :
                response.aggregations()
                        .get("top_airlines")
                        .sterms()
                        .buckets()
                        .array()) {

            long total = bucket.aggregations()
                    .get("total_delays")
                    .sum()
                    .value()
                    .longValue();

            System.out.println(bucket.key().stringValue() + ": " + total);
        }
    }
}
