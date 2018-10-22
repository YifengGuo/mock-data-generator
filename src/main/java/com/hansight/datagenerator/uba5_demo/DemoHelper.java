package com.hansight.datagenerator.uba5_demo;

import com.alibaba.fastjson.JSONObject;
import com.hansight.datagenerator.bigscreen.utils.ElasticsearchConnection;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by guoyifeng on 10/22/18
 */
@SuppressWarnings("Duplicates")
public class DemoHelper {

    private static final String SAAS_DEMO_INDEX = "saas_demo_20180811";

    private static final String USER_DEMO_INDEX = "user_demo";

    private static final String LOGON_TYPE = "logon";

    private static final String EMAIL_TYPE = "email";

    private static final String HTTP_TYPE = "http";

    private static final String FILE_TYPE = "file";

    private static final String DEFAULT_DATABASE_IP = "178.16.155.40";

    private ElasticsearchConnection connection;

    private static final String[] FREQUENT_PC = {"PC-111", "PC-222", "PC-333", "PC-444", "PC-555", "PC-666", "PC-777", "PC-888", "PC-999"};

    private static final String[] SUFFIX_ARR = {".doc", ".zip", ".pdf", ".jpg", ".rar", ".exe", ".txt", ".java"};

    private static final String[] FILENAMES = {"项目", "工资", "日程"};

    private static Map<String, String> PC_MAP =  new HashMap<>();

    private static final String TARGET_USER = "OEH0380";

    @Before
    public void initial() throws Exception {
        connection = new ElasticsearchConnection();
        connection.connect("es-jw-darpa", "172.16.150.149", 29300);
    }

    /**
     * add database field to saas_demo/logon
     */
    @Test
    public void addDataBase() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(LOGON_TYPE)
                .setQuery(QueryBuilders.matchAllQuery())
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(10000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            for (String id : ids) {
                connection.client().prepareUpdate(SAAS_DEMO_INDEX, LOGON_TYPE, id)
                        .setDoc(jsonBuilder()
                                .startObject()
                                .field("database", DEFAULT_DATABASE_IP)
                                .endObject())
                        .get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * modify frequently used pc for users
     */
    @Test
    public void modifyPC() throws Exception {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(LOGON_TYPE)
                .setQuery(QueryBuilders.existsQuery("pc"))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(10000)
                .get();

        Map<String, String> tmp = new HashMap<>();
        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
                tmp.put(hit.getId(), String.valueOf(hit.getSource().get("user")));
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }
        Set<String> set = new HashSet<>();
        try {
            for (String id : ids) {
                if (!set.contains(tmp.get(id))) {
                    int randIndex = new Random().nextInt(FREQUENT_PC.length);
                    connection.client().prepareUpdate(SAAS_DEMO_INDEX, LOGON_TYPE, id)
                            .setDoc(jsonBuilder()
                                    .startObject()
                                    .field("pc", FREQUENT_PC[randIndex])
                                    .endObject())
                            .get();
                    PC_MAP.put(tmp.get(id), FREQUENT_PC[randIndex]);
                    set.add(tmp.get(id));
                } else {
                    connection.client().prepareUpdate(SAAS_DEMO_INDEX, LOGON_TYPE, id)
                            .setDoc(jsonBuilder()
                                    .startObject()
                                    .field("pc", PC_MAP.get(tmp.get(id)))
                                    .endObject())
                            .get();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();

//        SearchResponse response2 = connection.client().prepareSearch(SAAS_DEMO_INDEX)
//                .setTypes(LOGON_TYPE)
//                .setQuery(QueryBuilders.existsQuery("pc"))
//                .setScroll(TimeValue.timeValueMinutes(1L))
//                .setSize(10000)
//                .get();
//
//        SearchHit[] hits2 = response2.getHits().getHits();
//        while (hits2 != null && hits2.length > 0) {
//            for (SearchHit hit : hits2) {
//                PC_MAP.put(String.valueOf(hit.getSource().get("user")), String.valueOf(hit.getSource().get("pc")));
//            }
//
//            response2 = connection.client().prepareSearchScroll(response2.getScrollId())
//                    .setScroll(TimeValue.timeValueMinutes(1L))
//                    .get();
//            hits2 = response2.getHits().getHits();
//        }
//
//        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();

//        SearchResponse allResponse = connection.client().prepareSearch(SAAS_DEMO_INDEX)
//                .setQuery(QueryBuilders.existsQuery("pc"))
//                .setScroll(TimeValue.timeValueMinutes(1L))
//                .setSize(10000)
//                .get();
//
//
//        SearchHit[] allHits = allResponse.getHits().getHits();
//
//        while (allHits != null && allHits.length > 0) {
//            for (SearchHit hit : allHits) {
//                UpdateRequest updateRequest = new UpdateRequest();
//                updateRequest.index(SAAS_DEMO_INDEX);
//                updateRequest.type(hit.getType());
//                updateRequest.id(hit.getId());
//                updateRequest.doc(jsonBuilder()
//                        .startObject()
//                        .field("pc", PC_MAP.get(String.valueOf(hit.getSource().get("user"))))
//                        .endObject());
//                connection.client().update(updateRequest).get();
//            }
//
//            allResponse = connection.client().prepareSearchScroll(allResponse.getScrollId())
//                    .setScroll(TimeValue.timeValueMinutes(1L))
//                    .get();
//            allHits = allResponse.getHits().getHits();
//        }
//
//        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    /**
     * mock target user accesses to database with unusual device
     */
    @Test
    public void test() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(LOGON_TYPE)
                .setQuery(QueryBuilders.matchQuery("user", "OEH0380"))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(10)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            for (String id : ids) {
                connection.client().prepareUpdate(SAAS_DEMO_INDEX, LOGON_TYPE, id)
                        .setDoc(jsonBuilder()
                                .startObject()
                                .field("pc", "PC-XXX")
                                .endObject())
                        .get();
                connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    static class WordGenerator {
        //随机生成常见汉字
        public static String getRandomChar() {
//            String str = "";
//            int highCode;
//            int lowCode;
//
//            Random random = new Random();
//
//            highCode = (176 + Math.abs(random.nextInt(39))); //B0 + 0~39(16~55) 一级汉字所占区
//            lowCode = (161 + Math.abs(random.nextInt(93))); //A1 + 0~93 每区有94个汉字
//
//            byte[] b = new byte[2];
//            b[0] = (Integer.valueOf(highCode)).byteValue();
//            b[1] = (Integer.valueOf(lowCode)).byteValue();
//
//            try {
//                str = new String(b, "utf-8");
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//            }
//            return str;
            return String.valueOf((char)(0x4e00+(int)(Math.random()*(0x9fa5-0x4e00+1))));
        }
    }

    @Test
    public void testWordGenerator() {
        System.out.print(WordGenerator.getRandomChar());
    }

    @Test
    public void generateRandomEmailSubject() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(EMAIL_TYPE)
//                .setQuery(QueryBuilders.matchQuery("user", "OEH0380"))
                .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("user", TARGET_USER)))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            for (String id : ids) {
                int subjectSize = ThreadLocalRandom.current().nextInt(3, 16);
                int suffixIndex = new Random().nextInt(SUFFIX_ARR.length);
                String suffix = SUFFIX_ARR[suffixIndex];
                connection.client().prepareUpdate(SAAS_DEMO_INDEX, EMAIL_TYPE, id)
                        .setDoc(jsonBuilder()
                                .startObject()
                                .field("subject","test.doc")
                                .endObject())
                        .get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    private String randomFileName(int size, String suffix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(WordGenerator.getRandomChar());
        }
        sb.append(suffix);
        return sb.toString();
    }

    @Test
    public void quitSubmit() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(EMAIL_TYPE)
                .setQuery(QueryBuilders.matchQuery("user", TARGET_USER))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
                connection.client().prepareUpdate(SAAS_DEMO_INDEX, EMAIL_TYPE, ids.get(4))
                        .setDoc(jsonBuilder()
                                .startObject()
                                .field("subject", "离职申请.pdf")
                                .field("to", "GC_Morin@gmail.com")
                                .endObject())
                        .get();

        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    @Test
    public void accessJobWebsites() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(HTTP_TYPE)
                .setQuery(QueryBuilders.matchQuery("user", TARGET_USER))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            connection.client().prepareUpdate(SAAS_DEMO_INDEX, HTTP_TYPE, ids.get(0))
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("url", "www.linkedin.com")
                            .endObject())
                    .get();

            connection.client().prepareUpdate(SAAS_DEMO_INDEX, HTTP_TYPE, ids.get(8))
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("url", "www.51job.com")
                            .endObject())
                    .get();

        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    @Test
    public void copySecrete() {

            JSONObject obj = new JSONObject();
            obj.put("pc", "PC-777");
            obj.put("filename", "2018绝密.pdf");
            obj.put("role", "ElectricalEngineer");
            obj.put("ip", "124.28.243.1");
            obj.put("location", "上海");
            obj.put("department", "3 - Engineering");
            obj.put("user", TARGET_USER);
            obj.put("occur_time", "1534046231000");
            JSONObject geo = new JSONObject();
            geo.put("lon", "121.3997");
            geo.put("lat", "31.0456");
            obj.put("src_geo", geo);
//            IndexResponse response = connection.client().prepareIndex(SAAS_DEMO_INDEX, FILE_TYPE, UUID.randomUUID().toString())
//                    .setSource(
//                            .startObject()
//                            .field("src_geo",
//                                    jsonBuilder()
//                                            .startObject()
//                                                .field("lon", "121.3997")
//                                                .field("lat", "31.0456")
//                                            .endObject()
//                                        )
//                            .field("pc", "PC-777")
//                            .field("filename", "2018绝密.pdf")
//                            .field("role", "ElectricalEngineer")
//                            .field("occur_time", "1534046231000")
//                            .field("ip", "124.28.243.1")
//                            .field("location", "上海")
//                            .field("department", "3 - Engineering")
//                            .field("user", TARGET_USER)
//                            .endObject()
//            obj
////                    )
//                    .get();

            IndexResponse response = connection.client().prepareIndex(SAAS_DEMO_INDEX, FILE_TYPE, UUID.randomUUID().toString())
                    .setSource(obj).get();


        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    @Test
    public void updateTimestamp() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(EMAIL_TYPE)
                .setQuery(QueryBuilders.matchQuery("user", TARGET_USER))
                .setQuery(QueryBuilders.matchQuery("to", "GC_Morin@gmail.com"))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            connection.client().prepareUpdate(SAAS_DEMO_INDEX, EMAIL_TYPE, ids.get(0))
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("occur_time", "1534129208000")
                            .endObject())
                    .get();

        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    @Test
    public void updateFilename() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch(SAAS_DEMO_INDEX)
                .setTypes(FILE_TYPE)
                .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("user", TARGET_USER)))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                ids.add(hit.getId());
            }

            response = connection.client().prepareSearchScroll(response.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .get();
            hits = response.getHits().getHits();
        }

        try {
            for (String id : ids) {
                int size = ThreadLocalRandom.current().nextInt(3, 10);
                int keywordIndex = new Random().nextInt(FILENAMES.length);
                int suffixIndex = new Random().nextInt(SUFFIX_ARR.length);
                connection.client().prepareUpdate(SAAS_DEMO_INDEX, FILE_TYPE, id)
                        .setDoc(jsonBuilder()
                                .startObject()
                                .field("filename", randomFileName(size, FILENAMES[keywordIndex], SUFFIX_ARR[suffixIndex]))
                                .endObject())
                        .get();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        connection.client().admin().indices().prepareRefresh(SAAS_DEMO_INDEX).get();
    }

    private String randomFileName(int size, String keyword, String suffix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(WordGenerator.getRandomChar());
        }
        sb.append(keyword);
        sb.append(suffix);
        return sb.toString();
    }

    @Test
    public void fetchData() {
        List<String> ids = new ArrayList<>();
        SearchResponse response = connection.client().prepareSearch("saas_demo_20180811")
                .setTypes(EMAIL_TYPE)
                .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("user", TARGET_USER)))
                .setScroll(TimeValue.timeValueMinutes(1L))
                .setSize(1000)
                .get();
        System.out.print(response.getHits().getHits()[0].getSourceAsString());
    }

    @Test
    public void updateLowerCase() throws Exception {
        connection.client().prepareUpdate("ueba_settings", "user_info", "AWOqfj5wqoKXGlv_H1p5")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("id", "oeh0380")
                        .endObject())
                .get();

    }
}
