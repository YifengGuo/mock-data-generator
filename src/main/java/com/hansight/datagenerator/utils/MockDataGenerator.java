package com.hansight.datagenerator.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Created by guoyifeng on 10/15/18
 */
public abstract class MockDataGenerator {

    protected ObjectMapper mapper;

    protected ElasticsearchConnection connection;

    private static final String CLUSTER_NAME = "es-jw-darpa";

    private static final String HOSTS = "172.16.150.149";

    private static final int TCP_PORT = 29300;

    public MockDataGenerator() {
        mapper = new ObjectMapper();

        connection = new ElasticsearchConnection();
        try {
            connection.connect(CLUSTER_NAME, HOSTS, TCP_PORT);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public abstract void writeToES(String json, String index, String type, String id);

    public abstract void deleteExistedData();

    public abstract boolean generateData(int startIndex, Date date);

    protected static long getCurrentDayStart(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime startOfDay = localDateTime.with(LocalTime.MIN);
        return startOfDay.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
    }

    protected static long getCurrentDayEnd(Date date) {
        LocalDateTime localDateTime = dateToLocalDateTime(date);
        LocalDateTime endOfDay = localDateTime.with(LocalTime.MAX);
        return endOfDay.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
    }

    protected static LocalDateTime dateToLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    public void close() {
        connection.close();
    }

}
