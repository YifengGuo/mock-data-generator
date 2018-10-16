package com.hansight.datagenerator.model;

/**
 * Created by guoyifeng on 10/15/18
 */

import java.util.List;

/**
 * This class is designed for Nanjing big screen
 */
public class MockUser {

    private long alarm_level;

    private String department;

    private long end_opt_time;

    private long first_opt_time;

    private String group;

    private String id;

    private boolean mockup;

    private String name;

    private long occur_time;

    private long scenario_size;  // triggered scenarios count

    private List<String> mock_user_scenarios; // store MockScenario setting_id

    private long score;

    public long getAlarm_level() {
        return alarm_level;
    }

    public String getDepartment() {
        return department;
    }

    public long getEnd_opt_time() {
        return end_opt_time;
    }

    public long getFirst_opt_time() {
        return first_opt_time;
    }

    public String getGroup() {
        return group;
    }

    public String getId() {
        return id;
    }

    public boolean isMockup() {
        return mockup;
    }

    public String getName() {
        return name;
    }

    public long getOccur_time() {
        return occur_time;
    }

    public long getScenario_size() {
        return scenario_size;
    }

    public List<String> getMock_user_scenarios() {
        return mock_user_scenarios;
    }

    public long getScore() {
        return score;
    }

    public void setAlarm_level(long alarm_level) {
        this.alarm_level = alarm_level;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public void setEnd_opt_time(long end_opt_time) {
        this.end_opt_time = end_opt_time;
    }

    public void setFirst_opt_time(long first_opt_time) {
        this.first_opt_time = first_opt_time;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setMockup(boolean mockup) {
        this.mockup = mockup;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOccur_time(long occur_time) {
        this.occur_time = occur_time;
    }

    public void setScenario_size(long scenario_size) {
        this.scenario_size = scenario_size;
    }

    public void setMock_user_scenarios(List<String> mock_user_scenarios) {
        this.mock_user_scenarios = mock_user_scenarios;
    }

    public void setScore(long score) {
        this.score = score;
    }
}
