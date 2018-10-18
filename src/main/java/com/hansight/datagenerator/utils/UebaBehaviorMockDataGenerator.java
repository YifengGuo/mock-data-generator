package com.hansight.datagenerator.utils;

import com.hansight.datagenerator.model.MockBehavior;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by guoyifeng on 10/18/18
 */
public class UebaBehaviorMockDataGenerator extends MockDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(UebaBehaviorMockDataGenerator.class);

    private static final String UEBA_ALARM_INDEX = "ueba_alarm";

    private static final String ANOMALY_BEHAVIORS = "anomaly_behaviors";

    private static final String UEBA_SETTINGS = "ueba_settings";

    private static final String USER_INFO = "user_info";

    private static final int DEFAULT_COUNT = 1;

    public UebaBehaviorMockDataGenerator() {
        super();
    }

    /**
     * MockBehavior has coupling with both user and scenario
     * entity  -> user id
     * max(alarm_level) -> scenario alarm level
     * scenario -> an instance of MockScenario
     * scenario_id ->  MockScenario setting_id
     * timestamp shall be synchronous with corresponding user
     *
     * mock logic: first randomly choose an user who has triggered this behavior
     *             second check out the mock_user_scenarios list to randomly choose a scenario for this behavior
     *             must exist a behavior whose alarm_level == scenario alarm level
     * @return a valid mock behavior
     */
    public MockBehavior initial(int index) {
        MockBehavior curr = new MockBehavior();
        List<Map<String, Object>> users = getMockUsers();

        Map<String, Object> currUser = users.get(new Random().nextInt(users.size()));

        curr.setEntity(String.valueOf(currUser.get("id")));

        // set scenario logic block
        List<String> scenarioList = (List)currUser.get("mock_user_scenarios");
        if (scenarioList.size() > 1) {
            // when this user has multiple scenarios
            // randomly choose one as scenario
            String randomScenarioEsId = scenarioList.get(new Random().nextInt(scenarioList.size()));
            Map<String, Object> randomScenario = getScenarioById(randomScenarioEsId);
            curr.setScenario(String.valueOf(randomScenario.get("scenario")));  // scenario name
            curr.setScenario_id(String.valueOf(randomScenario.get("scenario_setting_id"))); // scenario setting id
        } else { // only has one scenario
            String scenarioEsId = scenarioList.get(0);
            Map<String, Object> uniqueScenario = getScenarioById(scenarioEsId);
            curr.setScenario(String.valueOf(uniqueScenario.get("scenario")));  // scenario name
            curr.setScenario_id(String.valueOf(uniqueScenario.get("scenario_setting_id"))); // scenario setting id
        }

        curr.setCount(DEFAULT_COUNT);
        curr.setDescription("Mock Desc");
        curr.setDetector("Mock Detector");
        curr.setGroup(String.valueOf(currUser.get("group")));
        curr.setOccur_time(Long.parseLong(String.valueOf(currUser.get("occur_time"))));
        curr.setEndTimestamp(Long.parseLong(String.valueOf(currUser.get("end_opt_time"))));
        curr.setMockup(true);
        curr.setModified(Long.parseLong(String.valueOf(currUser.get("end_opt_time"))));

    }

    @Override
    public void writeToES(String json, String index, String type, String id) {

    }

    @Override
    protected void deleteExistedData() {

    }

    @Override
    protected boolean generateData(int startIndex, Date date) {
        return false;
    }

    @Override
    protected void close() {
        super.close();
    }

    private List<Map<String, Object>> getMockUsers() {
        SearchResponse response = connection.client.prepareSearch(UEBA_SETTINGS)
                .setTypes(USER_INFO)
                .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("alarm_level").gt(0)))
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .get();

        List<Map<String, Object>> res = new ArrayList<>();

        for (SearchHit hit : response.getHits().getHits()) {
            res.add(hit.getSource());
        }
        return res;
    }

    private List<Map<String, Object>> getMockScenarios() {
        SearchResponse response = connection.client.prepareSearch(UEBA_ALARM_INDEX)
                .setTypes(ANOMALY_BEHAVIORS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .get();

        List<Map<String, Object>> res = new ArrayList<>();

        for (SearchHit hit : response.getHits().getHits()) {
            res.add(hit.getSource());
        }
        return res;
    }

    private Map<String, String> getScenarioIdAndName(List<Map<String, Object>> scenarios, String _id) {
        Map<String, String> res = new HashMap<>();

    }

    private Map<String, Object> getScenarioById(String _id) {
        GetResponse response = connection.client.prepareGet(UEBA_ALARM_INDEX, ANOMALY_BEHAVIORS, _id).get();
        if (response.isExists()) {
            return response.getSource();
        } else {
            return new HashMap<>();
        }
    }
}
