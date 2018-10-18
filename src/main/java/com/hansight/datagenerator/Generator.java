package com.hansight.datagenerator;

/**
 * Created by guoyifeng on 10/16/18
 */

import com.hansight.datagenerator.utils.UebaAlarmMockDataGenerator;
import com.hansight.datagenerator.utils.UebaBehaviorMockDataGenerator;
import com.hansight.datagenerator.utils.UebaUserMockDataGenerator;
import org.elasticsearch.action.search.SearchResponse;

import java.util.Date;
import java.util.List;

/**
 * entry point of this project
 */
public class Generator {

    private static final int DEFAULT_SCENARIO_START_INDEX = 1;

    private static final int DEFAULT_USER_START_INDEX = 1;  // better always set to 1

    private static final Date TODAY = new Date();

    private static final int WEEK_DAYS = 7;

    private static final long DAY_MILLIS = 24 * 60 * 60 * 1000;


    public static void main(String[] args) {
//        run(DEFAULT_SCENARIO_START_INDEX, DEFAULT_USER_START_INDEX, TODAY);
//        run(2, DEFAULT_USER_START_INDEX, new Date(System.currentTimeMillis()-24*60*60*1000));
        customGenerate(DEFAULT_USER_START_INDEX, 1, TODAY);
    }

    /**
     * by default: generate mock data by today's date
     * always delete existed user data and scenarios data
     */
    public static void run(int scenarioStartIndex, int userStartIndex, Date date) {
        // initialize generators
        UebaAlarmMockDataGenerator scenarioGenerator = new UebaAlarmMockDataGenerator();
        UebaUserMockDataGenerator userGenerator = new UebaUserMockDataGenerator();

        // optional: delete existed data in the elasticsearch
        // if unnecessary, please comment this block
        scenarioGenerator.deleteExistedData();
        userGenerator.deleteExistedData();

        // write mock data to the elasticsearch
        // CAUTIONS: scenarios shall be write to the es BEFORE writing mock users to the es
        scenarioGenerator.generateData(scenarioStartIndex, date);
        userGenerator.generateData(userStartIndex, date);

        // close connection of elasticsearch
        scenarioGenerator.close();
        userGenerator.close();

    }

    /**
     *
     * @param userStartIndex
     * @param period  means how many days before today. Normally I set it to 7 for test
     * @param date
     * currently my setting is to generate several users with 11 abnormal ones and 6 scenarios which vary in a week
     * so totally es has 42 scenarios
     */
    public static void customGenerate(int userStartIndex, int period, Date date) {
        // initialize generators
        UebaAlarmMockDataGenerator scenarioGenerator = new UebaAlarmMockDataGenerator();
        UebaUserMockDataGenerator userGenerator = new UebaUserMockDataGenerator();
        UebaBehaviorMockDataGenerator behaviorGenerator = new UebaBehaviorMockDataGenerator();

        // optional: delete existed data in the elasticsearch
        // if unnecessary, please comment this block
        scenarioGenerator.deleteExistedData();
        userGenerator.deleteExistedData();
        behaviorGenerator.deleteExistedData();

        // write mock data to the elasticsearch
        // CAUTIONS: scenarios shall be write to the es BEFORE writing mock users to the es
        for (int i = DEFAULT_SCENARIO_START_INDEX; i <= period; i++) {
            scenarioGenerator.generateData(i, calculateDate(i));
        }
        userGenerator.generateData(userStartIndex, date);
        behaviorGenerator.generateData(userStartIndex, date);
        userGenerator.updateUserScore(); // update user's total score after behaviors are generated

        // close connection of elasticsearch
        scenarioGenerator.close();
        userGenerator.close();
        behaviorGenerator.close();
    }

    private static Date calculateDate(int offset) {
        return new Date(System.currentTimeMillis()- 24 * 60 * 60 * 1000 * offset);
    }
}
