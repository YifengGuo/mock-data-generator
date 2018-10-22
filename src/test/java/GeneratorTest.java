import com.hansight.datagenerator.bigscreen.utils.ElasticsearchConnection;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Created by guoyifeng on 10/18/18
 */
public class GeneratorTest {
    @Test
    public void test() throws Exception {
        ElasticsearchConnection connection = new ElasticsearchConnection();
        connection.connect("es-jw-darpa", "172.16.150.149", 29300);
        QueryBuilder qb = QueryBuilders.boolQuery();
        ((BoolQueryBuilder) qb).must(QueryBuilders.termQuery("mockup", "true"));
        ((BoolQueryBuilder) qb).must(QueryBuilders.rangeQuery("first_opt_time").lte(System.currentTimeMillis()));
        SearchResponse response = connection.client().prepareSearch("ueba_settings")
                .setTypes("user_info")
                .setQuery(qb)
                .setSize(1000)
                .get();

        Arrays.stream(response.getHits().getHits()).forEach(hit -> System.out.println(hit.getSource()));
    }
}
