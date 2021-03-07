package com.tj.demo.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.tj.demo.elasticsearch.config.EsConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ElasticsearchApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 查看是否注入成功
     */
    @Test
    void contextLoads() {
        System.out.println(client);
    }

    /**
     * 新建索引并存储数据
     */
    @Test
    void indexTest() throws IOException {
        // 创建索引对象，索引名为user,id为1
        IndexRequest indexRequest = new IndexRequest("user").id("1");
        User user = new User("zhangsan", "男", 12);
        String s = JSON.toJSONString(user);
        // 设置数据
        indexRequest.source(s, XContentType.JSON);
        // 执行操作
        IndexResponse indexResponse = client.index(indexRequest, EsConfig.COMMON_OPTIONS);
        System.out.println(indexResponse);
    }

    /**
     * 查询数据
     */
    @Test
    void searchTest() throws IOException {
        //创建检索请求
        SearchRequest searchRequest = new SearchRequest();
        //指定索引
        searchRequest.indices("bank");
        // 创建检索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("address", "mill"));//查询
        searchSourceBuilder.aggregation(AggregationBuilders.terms("ageAgg").field("age"));
//        searchSourceBuilder.from(2);
//        searchSourceBuilder.size(2);
        searchRequest.source(searchSourceBuilder);
        // 执行检索
        SearchResponse response = client.search(searchRequest, EsConfig.COMMON_OPTIONS);
        System.out.println(response);//所有返回数据
//        Map map = JSON.parseObject(response.toString(), Map.class);转为map
//        System.out.println(map.get("aggregations"));

        // 获取聚合信息
        Aggregations aggregations = response.getAggregations();
        Terms ageAgg = aggregations.get("ageAgg");
        for (Bucket bucket : ageAgg.getBuckets()) {
            System.out.println("key:" + bucket.getKeyAsString());
            System.out.println("DocCount:" + bucket.getDocCount());
        }

        // hits信息
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(Arrays.toString(hits));

        // source数据
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }
}

@Data
@AllArgsConstructor
class User {

    private String name;
    private String sex;
    private Integer age;
}
