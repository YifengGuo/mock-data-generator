package com.hansight.datagenerator.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hansight.datagenerator.model.MockUser;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by guoyifeng on 10/15/18
 */
public class UebaUserMockDataGenerator extends MockDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(UebaUserMockDataGenerator.class);

    private static final String UEBA_ALARM_INDEX = "ueba_alarm";

    private static final String ANOMALY_SCENARIOS = "anomaly_scenarios";

    private static final String UEBA_SETTINGS = "ueba_settings";

    private static final String USER_INFO = "user_info";

    public static final int USERS_LIMIT = 11;

    private AtomicInteger counter = new AtomicInteger(0);

    public UebaUserMockDataGenerator() {
        super();
    }

    @Override
    // generate fake user data
    public boolean generateData(int startIndex, Date date) {
        if (startIndex <= 0) {
            throw new IllegalArgumentException("startIndex should start from 1.");
        }
        int index = startIndex;
        while (true) {
            // currently we have 11 anomaly users already
            if (counter.get() == USERS_LIMIT) {
                return true;
            }
            MockUser curr = initial(index, date);
            writeToES(jsonify(curr), UEBA_SETTINGS, USER_INFO, String.valueOf(index));
            if (curr.getScore() != 0L) {
                counter.incrementAndGet();
            }
            index++;
        }
    }

    @Override
    public void deleteExistedData() {
        SearchResponse response = connection.client().prepareSearch(UEBA_SETTINGS)
                .setTypes(USER_INFO)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setSize(1000)
                .get();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            ids.add(String.valueOf(response.getHits().getHits()[i].getId()));
        }

        for (String id : ids) {
            DeleteResponse deleteResponse = connection.client().prepareDelete(UEBA_SETTINGS, USER_INFO, id).get();
            LOG.info("mock user {} is deleted", deleteResponse.getId());
        }

    }

    public MockUser initial(int index, Date date) {
        MockUser curr = new MockUser();
        curr.setDepartment("mock department");
        curr.setFirst_opt_time(getCurrentDayStart(date));
        curr.setEnd_opt_time(getCurrentDayEnd(date));
        curr.setGroup("mock group");
        curr.setId((String.valueOf(index)));
        curr.setMockup(true);
        curr.setOccur_time(getCurrentDayStart(date));
        curr.setName("User_" + index);
        // randomly set current user has triggered ... scenarios. at most 3, at least 0
        curr.setScenario_size(ThreadLocalRandom.current().nextInt(0, 4));

        curr.setMock_user_scenarios(generateRandomScenarios(curr));

        curr.setScore(calculateScore(curr.getMock_user_scenarios()));

        curr.setAlarm_level(determineAlarmLevel(curr));

        if (curr.getScore() == 0L) {
            curr.setAlert_size(0L);
        } else {
            curr.setAlert_size(ThreadLocalRandom.current().nextLong(curr.getScenario_size(), 3 * curr.getScenario_size()));
        }

        return curr;
    }

    private String jsonify(MockUser curr) {
        try {
            return this.mapper.writeValueAsString(curr);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage());
        }
        LOG.error("error in jsonify process");
        return "";
    }

    private long determineAlarmLevel(MockUser curr) {
        if (curr.getScore() == 0) {
            return 0L;
        }
        // fetch corresponding scenarios from es
        // find the largest alarm_level among them and set it to the current user
        List<String> scenarioIds = curr.getMock_user_scenarios();
        long maxLevel = scenarioIds.stream()
                .map(id -> Long.parseLong(String.valueOf(connection.client.prepareGet(UEBA_ALARM_INDEX, ANOMALY_SCENARIOS, id).get().getSource().get("alarm_level"))))
                .max(Comparator.comparingLong(i -> i)).orElseThrow(NoSuchElementException::new);

        return maxLevel;
//        long totalScore = curr.getScore();
//        long size = curr.getScenario_size();
//        if (totalScore == 0) {
//            return 0;
//        }
//        long avgScore = totalScore / size;
//        if (avgScore > 0 && avgScore < 30) {
//            return 1;
//        } else if (avgScore >= 30 && avgScore < 60) {
//            return 2;
//        } else if (avgScore >= 60 && avgScore < 90) {
//            return 3;
//        } else {
//            return 4;
//        }
    }

    private List<String> generateRandomScenarios(MockUser curr) {
        if (curr.getScenario_size() == 0) {
            return new ArrayList<>();
        }

        int size = (int) curr.getScenario_size();
        List<String> res = new ArrayList<>(size);

        // randomly assign MockScenario _id to this user
        List<String> tmp = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(UEBA_ALARM_INDEX).setTypes(ANOMALY_SCENARIOS)
                .setQuery(QueryBuilders.termQuery("mockup", true))
                .setSize(1000)
                .get();

//        LOG.info("response size is {}", response.getHits().getHits().length);
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            String id = String.valueOf(response.getHits().getHits()[i].getId());
            String scenarioName = String.valueOf(response.getHits().getHits()[i].getSource().get("scenario"));
            tmp.add(id);
        }
//        LOG.info("tmp size is {}", tmp.size());
        for (int i = 0; i < size; i++) {
            Set<String> dedup = new HashSet<>();
            int currIndex = new Random().nextInt(tmp.size());
            // guarantee an user will not trigger one scenario more than once
            while (dedup.contains(tmp.get(currIndex))) {
                currIndex = new Random().nextInt(tmp.size());
            }
            dedup.add(tmp.get(currIndex));
            res.add(tmp.get(currIndex));
        }

        return res;
    }

//    private List<String> convertIdToName(List<String> res) {
//        return res.stream()
//                .map(s -> idNameMap.get(s))
//                .collect(Collectors.toList());
//    }

    private long calculateScore(List<String> scenarioIds) {
        long totalScore = 0L;

        for (String s : scenarioIds) {
            GetResponse response = connection.client().prepareGet(UEBA_ALARM_INDEX, ANOMALY_SCENARIOS, s).get();
            totalScore += Long.parseLong(String.valueOf(response.getSource().get("score")));
        }
        return totalScore;
    }


    @Override
    public void writeToES(String json, String index, String type, String id) {
        IndexResponse response = connection.client().prepareIndex(index, type, id)
                .setSource(json)
                .get();
        LOG.info("user index repsonse is {}", response.getId());
    }

    @Override
    public void close() {
        super.close();
    }
}
