package com.hansight.datagenerator.model;

/**
 * Created by guoyifeng on 10/18/18
 */

/**
 * Mock Behaviors of Mock Scenario
 * each Scenario may contain multiple behaviors
 * Scenario's alarm_level is determined by max alarm_level of behaviors belonging to this scenario
 */
public class MockBehavior {

    private long alarm_level;

    private long count;

    private String description;

    private String detector;

    private long endTimestamp;

    private String entity; //  mapping to id of user_info

    private String group;

    private boolean mockup;

    private long modified; // last modified time

    private long occur_time;

    private String scenario;

    private String scenario_id;

    private long score; // score = weight * scenario's alarm_level

    public long getAlarm_level() {
        return alarm_level;
    }

    public void setAlarm_level(long alarm_level) {
        this.alarm_level = alarm_level;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDetector() {
        return detector;
    }

    public void setDetector(String detector) {
        this.detector = detector;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isMockup() {
        return mockup;
    }

    public void setMockup(boolean mockup) {
        this.mockup = mockup;
    }

    public long getModified() {
        return modified;
    }

    public void setModified(long modified) {
        this.modified = modified;
    }

    public long getOccur_time() {
        return occur_time;
    }

    public void setOccur_time(long occur_time) {
        this.occur_time = occur_time;
    }

    public String getScenario() {
        return scenario;
    }

    public void setScenario(String scenario) {
        this.scenario = scenario;
    }

    public String getScenario_id() {
        return scenario_id;
    }

    public void setScenario_id(String scenario_id) {
        this.scenario_id = scenario_id;
    }

    public long getScore() {
        return score;
    }

    public void setScore(long score) {
        this.score = score;
    }
}
