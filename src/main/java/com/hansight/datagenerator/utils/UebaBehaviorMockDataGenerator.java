package com.hansight.datagenerator.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hansight.datagenerator.model.MockBehavior;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by guoyifeng on 10/18/18
 */
public class UebaBehaviorMockDataGenerator extends MockDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(UebaBehaviorMockDataGenerator.class);

    private static final String UEBA_ALARM_INDEX = "ueba_alarm";

    private static final String ANOMALY_BEHAVIORS = "anomaly_behaviors";

    private static final String ANOMALY_SCENARIOS = "anomaly_scenarios";

    private static final String UEBA_SETTINGS = "ueba_settings";

    private static final String USER_INFO = "user_info";

    private static final int DEFAULT_COUNT = 1;

    private static final double WEIGHT = 1.1;

    private static final int SCENARIO_COUNT = 6;

    private static Set<String> alarmLevelFlag = new HashSet<>();  // if one of scenarios's alarm_level has been covered by a behavior, add its setting_id to this set

    public UebaBehaviorMockDataGenerator() {
        super();
    }

    @Override
    public boolean generateData(int startIndex, Date date) {
        List<Map<String, Object>> users = getMockUsers();
        LOG.info("user size {}", users.size());
        while (true) {
            for (int userIndex = 0; userIndex < users.size(); userIndex++) {  // iterate on users
                List<String> scenarioList = (List)users.get(userIndex).get("mock_user_scenarios");
                for (int scenarioIndex = 0; scenarioIndex < scenarioList.size(); scenarioIndex++) { // iterate on current user's scenario list
                    // mock behavior based on each scenario this user has
                    // each scenario may have multiple behaviors -> currently set it within [1, 3]
                    int behaviorCount = ThreadLocalRandom.current().nextInt(6, 10);
                    for (int i = 0; i < behaviorCount; i++) {
                        MockBehavior curr = initial(users, userIndex, scenarioList, scenarioIndex);
                        if (curr != null) {
                            writeToES(jsonify(curr), UEBA_ALARM_INDEX, ANOMALY_BEHAVIORS, UUID.randomUUID().toString());
                        }
                    }
                }
            }
            if (alarmLevelFlag.size() == SCENARIO_COUNT) {
                LOG.info("behaviors created successfully with corresponding scenarios alarm level correctly generated.");
                connection.client.admin().indices().prepareRefresh(UEBA_ALARM_INDEX).get();
                return true;
            } else {
                alarmLevelFlag.clear();  // reset alarmLevelFlag
                deleteExistedData();  // delete existed data
                LOG.info("behaviors are not all valid, recreating...");
            }
        }
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
    public MockBehavior initial(List<Map<String, Object>> users, int userIndex, List<String> scenarioList, int scenarioIndex) {
        MockBehavior curr = new MockBehavior();

        Map<String, Object> currUser = users.get(userIndex);  // outer loop iterates on currUser
        LOG.info("user id is: {}", currUser.get("id"));
        curr.setEntity(String.valueOf(currUser.get("id")));

        String scenarioEsId = scenarioList.get(scenarioIndex);
        Map<String, Object> uniqueScenario = getScenarioById(scenarioEsId);
        curr.setScenario(String.valueOf(uniqueScenario.get("scenario")));  // scenario name
        curr.setScenario_id(String.valueOf(uniqueScenario.get("scenario_setting_id"))); // scenario setting id

        if (!uniqueScenario.isEmpty()) {
            // set alarm level randomly, if current behavior alarm_level == scenario alarm_level, this scenario is complete with alarm_level
            int randomAlarmLevel = ThreadLocalRandom.current().nextInt(1, Integer.parseInt(String.valueOf(uniqueScenario.get("alarm_level"))) + 1);
            curr.setAlarm_level(randomAlarmLevel);
            if (randomAlarmLevel == Integer.parseInt(String.valueOf(uniqueScenario.get("alarm_level")))) {
                alarmLevelFlag.add(String.valueOf(uniqueScenario.get("scenario_setting_id")));
            }
        } else {
            return null;
        }

        curr.setCount(DEFAULT_COUNT);
        curr.setDescription("Mock Desc");
        curr.setDetector("Mock Detector");
        curr.setGroup(String.valueOf(currUser.get("group")));
        curr.setOccur_time(Long.parseLong(String.valueOf(currUser.get("occur_time"))));
        curr.setEndTimestamp(Long.parseLong(String.valueOf(currUser.get("end_opt_time"))));
        curr.setMockup(true);
        curr.setModified(Long.parseLong(String.valueOf(currUser.get("end_opt_time"))));
        curr.setScore((long)(WEIGHT * Long.parseLong(String.valueOf(uniqueScenario.get("score")))));

        return curr;
    }

    @Override
    public void writeToES(String json, String index, String type, String id) {
        IndexResponse response = connection.client().prepareIndex(index, type, id)
                .setSource(json)
                .get();
        LOG.info("behavior index response is {}", response.getId());
    }

    @Override
    public void deleteExistedData() {
        SearchResponse response = connection.client().prepareSearch(UEBA_ALARM_INDEX)
                .setTypes(ANOMALY_BEHAVIORS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setSize(1000)
                .get();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            ids.add(String.valueOf(response.getHits().getHits()[i].getId()));
        }

        for (String id : ids) {
            DeleteResponse deleteResponse = connection.client().prepareDelete(UEBA_ALARM_INDEX, ANOMALY_BEHAVIORS, id).get();
            LOG.info("mock behavior {} is deleted", deleteResponse.getId());
        }

        connection.client.admin().indices().prepareRefresh(UEBA_ALARM_INDEX).get();

    }

    @Override
    public void close() {
        super.close();
    }

    private List<Map<String, Object>> getMockUsers() {
        connection.client.admin().indices().prepareRefresh(UEBA_SETTINGS).get();
        SearchResponse response = connection.client.prepareSearch(UEBA_SETTINGS)
                .setTypes(USER_INFO)
                .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("alarm_level").gt(0)))
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setSize(1000)
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
                .setSize(1000)
                .get();

        List<Map<String, Object>> res = new ArrayList<>();

        for (SearchHit hit : response.getHits().getHits()) {
            res.add(hit.getSource());
        }
        return res;
    }

//    private Map<String, String> getScenarioIdAndName(List<Map<String, Object>> scenarios, String _id) {
//        Map<String, String> res = new HashMap<>();
//
//    }

    private Map<String, Object> getScenarioById(String _id) {
        GetResponse response = connection.client.prepareGet(UEBA_ALARM_INDEX, ANOMALY_SCENARIOS, _id).get();
        if (response.isExists() && Long.parseLong(String.valueOf(response.getSource().get("alarm_level"))) > 0) {
            return response.getSource();
        } else {
            return new HashMap<>();
        }
    }

    private String jsonify(MockBehavior behavior) {
        try {
            return this.mapper.writeValueAsString(behavior);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage());
        }
        LOG.error("error in jsonify process");
        return "";
    }
}
