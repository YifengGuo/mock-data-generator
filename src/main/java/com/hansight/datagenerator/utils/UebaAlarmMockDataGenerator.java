package com.hansight.datagenerator.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hansight.datagenerator.model.MockScenario;
import com.oracle.tools.packager.Log;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by guoyifeng on 10/15/18
 */
public class UebaAlarmMockDataGenerator extends MockDataGenerator {

    Logger LOG = LoggerFactory.getLogger(UebaAlarmMockDataGenerator.class);

    private static final String UEBA_ALARM_INDEX = "ueba_alarm";

    private static final String ANOMALY_SCENARIOS = "anomaly_scenarios";

    private static final int SCENARIO_LIMIT = 6;

    public List<Long> set = new ArrayList<>();

    private static Map<String, String> settingIdMap = new HashMap<>();

    public UebaAlarmMockDataGenerator() {
        super();
    }

    /**
     *
     * @param startIndex mock different days scenarios with different ids
     * @param date
     */
    @Override
    public boolean generateData(int startIndex, Date date) {
        if (startIndex <= 0) {
            throw new IllegalArgumentException("startIndex should start from 1!");
        }
        while (true) {
            for (int i = (startIndex - 1) * SCENARIO_LIMIT + 1; i <= startIndex * SCENARIO_LIMIT; i++) {
                int weeklyOccur = ThreadLocalRandom.current().nextInt(2, 8);  // a scenario would be triggered in 3 to 6 of a week days
                // firstly randomly assign a day that current scenario will be triggered
                // secondly randomly assign a count that current scenario will be triggered for this count times
                for (int j = 0; j < weeklyOccur; j++) {
                    int dailyRandom = ThreadLocalRandom.current().nextInt(2, 20);
                    for (int k = 0; k < dailyRandom; k++) {
                        MockScenario currScenario = initialMockScenario(i, new Date(date.getTime() - j * 24 * 60 * 60 * 1000));
                        writeToES(jsonify(currScenario), UEBA_ALARM_INDEX, ANOMALY_SCENARIOS, UUID.randomUUID().toString());
                    }
                }

            }
            if (set.size() == 4) {
                LOG.info("Scenarios generated with all alarm levels covered.");
                connection.client.admin().indices().prepareRefresh(UEBA_ALARM_INDEX).get();
                return true;
            } else {
                set.clear();
                set = new ArrayList<>();
                LOG.info("did not successfully create 6 scenarios cover all alarm levels. redoing...");
            }
        }
    }

    @Override
    public void deleteExistedData() {
        SearchResponse response = connection.client().prepareSearch(UEBA_ALARM_INDEX)
                .setTypes(ANOMALY_SCENARIOS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setSize(1000)
                .get();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            ids.add(String.valueOf(response.getHits().getHits()[i].getId()));
        }

        for (String id : ids) {
            DeleteResponse deleteResponse = connection.client().prepareDelete(UEBA_ALARM_INDEX, ANOMALY_SCENARIOS, id).get();
            LOG.info("mock scenario {} is deleted", deleteResponse.getId());
        }
        connection.client.admin().indices().prepareRefresh(UEBA_ALARM_INDEX).get();
    }

    /**
     * occur_time current day 00:00
     * end_time current day 24:00
     * scenario, scenario_desc, advice shall be different
     * alarm_level shall be different and cover all 4 kinds
     * @param index
     * @param date if needs today's mock data, this should be set with current epoch mills time
     * @return
     */
    private MockScenario initialMockScenario(int index, Date date) {
        MockScenario curr = new MockScenario();
        curr.setOccur_time(getCurrentDayStart(date));
        curr.setEnd_time(getCurrentDayEnd(date));
        long alarmLevel = new Random().nextInt(5);
        curr.setAlarm_level(alarmLevel);
        if (alarmLevel != 0L && !set.contains(alarmLevel)) {
            set.add(alarmLevel);
        }
        curr.setMockup(true);
        curr.setPeriod(86400000);
        curr.setScore(getRandomScore(curr));
        if (index <= SCENARIO_LIMIT) {
            index = index;
        } else {
            index = ((index) % SCENARIO_LIMIT != 0) ? (index % SCENARIO_LIMIT) : SCENARIO_LIMIT; // guarantee there are always 6 scenarios in total but with different id and timestamp
        }
        curr.setScenario("mock_test_" + index);

        connection.client.admin().indices().prepareRefresh(UEBA_ALARM_INDEX).get();

        // scenario with same scenario name shall have same setting_id
//        Map<String, Object> tmpMap = utilMap(index);
//        if ((boolean)tmpMap.get("isExisted") && !tmpMap.get("settingId").equals("")) {
//            curr.setScenario_setting_id(String.valueOf(tmpMap.get("settingId")));
//        } else {
//            curr.setScenario_setting_id(UUID.randomUUID().toString());
//            settingIdMap.put(String.valueOf(curr.getScenario()), curr.getScenario_setting_id());
//        }
        if (isExisted(index)) {
            curr.setScenario_setting_id(settingIdMap.get(String.valueOf(curr.getScenario())));
        } else {
            curr.setScenario_setting_id(UUID.randomUUID().toString());
            settingIdMap.put(String.valueOf(curr.getScenario()), curr.getScenario_setting_id());
        }
        curr.setModified(System.currentTimeMillis()); // set last modified time with now
        return curr;
    }

    private String jsonify(MockScenario scenario) {
        try {
            return this.mapper.writeValueAsString(scenario);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage());
        }
        LOG.error("error in jsonify process");
        return "";
    }

    @Override
    public void writeToES(String json, String index, String type, String id) {
        IndexResponse response = connection.client().prepareIndex(index, type, id)
                .setSource(json)
                .get();
        LOG.info("scenario index response is {}", response.getId());
    }

    // return a valid score of scenario based on its alarm_level
    private long getRandomScore(MockScenario curr) {
        if (curr.getAlarm_level() == 0) {
            return 0L;
        } else if (curr.getAlarm_level() == 1) {
            return ThreadLocalRandom.current().nextLong(0, 30);
        } else if (curr.getAlarm_level() == 2) {
            return ThreadLocalRandom.current().nextLong(31, 60);
        } else if (curr.getAlarm_level() == 3) {
            return ThreadLocalRandom.current().nextLong(61, 90);
        } else if (curr.getAlarm_level() == 4) {
            return ThreadLocalRandom.current().nextLong(91, 100);
        }
        return -1;
    }

    private Map<String, Object> utilMap(int index) {
        Map<String, Object> res = new HashMap<>();
        SearchResponse response = connection.client.prepareSearch(UEBA_ALARM_INDEX)
                .setTypes(ANOMALY_SCENARIOS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setQuery(QueryBuilders.termQuery("scenario", "mock_test_" + index))
                .setSize(1000)
                .get();

        if (response.getHits().getHits().length == 0) {
            res.put("isExisted", false);
            res.put("settingId", "");
            return res;
        } else {
            res.put("isExisted", true);
            res.put("settingId", String.valueOf(response.getHits().getHits()[0].getSource().get("scenario_setting_id")));
            return res;
        }
    }

    private boolean isExisted(int index) {
        SearchResponse response = connection.client.prepareSearch(UEBA_ALARM_INDEX)
                .setTypes(ANOMALY_SCENARIOS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setQuery(QueryBuilders.termQuery("scenario", "mock_test_" + index))
                .setSize(1000)
                .get();
        return response.getHits().getHits().length > 0;
    }

    @Override
    public void close() {
        super.close();
    }
}
