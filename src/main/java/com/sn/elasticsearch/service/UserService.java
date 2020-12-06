package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sn.elasticsearch.bean.User;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class UserService {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 创建索引
     *
     * @throws IOException
     */
    public void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("user");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 创建索引时指定一些配置信息
     *
     * @throws IOException
     */
    public void createIndex2() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("user");

        // 索引分片数量配置
        request.settings(Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1));

        // 设置文档字段的映射信息
        Map<String, Object> birthday = new HashMap<>();
        birthday.put("type", "date");
        birthday.put("format", "yyyy-MM-dd");
        Map<String, Object> properties = new HashMap<>();
        properties.put("birthday", birthday);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);

        // 通过json设置文档字段的映射信息
//        request.mapping("{\n" +
//                "   \"properties\": {\n" +
//                "       \"birthday\": {\n" +
//                "           \"type\": \"date\",\n" +
//                "           \"format\": \"yyyy-MM-dd\"\n" +
//                "       }\n" +
//                "   }\n" +
//                "}", XContentType.JSON);

        // 设置索引别名
        request.alias(new Alias("user_alias"));

        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 判断索引是否存在
     *
     * @return
     * @throws IOException
     */
    public boolean existsIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("user");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    public void deleteIndex() throws IOException {
        if (!existsIndex()) {
            return;
        }
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }

    /**
     * 添加单个文档
     *
     * @throws IOException
     */
    public void addDocument() throws IOException {
        User user = new User();
        user.setName("张三");
        user.setAge(30);
        user.setBirthday("1990-03-12");
        user.setSchool("清华");

        IndexRequest request = new IndexRequest("user");
//        request.timeout(TimeValue.timeValueSeconds(2));
        // 超时时间
        request.timeout("2s");
        // 文档id
        request.id("1");
        request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 批量添加文档
     *
     * @throws IOException
     */
    public void bulkAddDocument() throws IOException {
        User user1 = new User();
        user1.setName("李四");
        user1.setAge(18);
        user1.setBirthday("2002-01-08");
        user1.setSchool("北大");

        User user2 = new User();
        user2.setName("王五");
        user2.setAge(25);
        user2.setBirthday("1995-02-05");
        user2.setSchool("北大");

        User user3 = new User();
        user3.setName("赵六");
        user3.setAge(43);
        user3.setBirthday("1977-04-03");
        user3.setSchool("复旦");

        User user4 = new User();
        user4.setName("张三丰");
        user4.setAge(80);
        user4.setBirthday("1940-08-15");
        user4.setSchool("复旦");

        User user5 = new User();
        user5.setName("王重阳");
        user5.setAge(70);
        user5.setBirthday("1950-07-07");
        user5.setSchool("清华");

        Object[] users = new Object[]{user1, user2, user3, user4, user5};

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("5s");

        for (int i = 0; i < users.length; i++) {
            String id = String.valueOf(i + 2);
            String source = JSON.toJSONString(users[i]);
            bulkRequest.add(new IndexRequest("user").id(id).source(source, XContentType.JSON));
        }

        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.status());
    }

    /**
     * 判断文档是否存在
     *
     * @return
     * @throws IOException
     */
    public boolean existsDocument() throws IOException {
        GetRequest request = new GetRequest("user", "1");
        // 不获取_source的内容
        request.fetchSourceContext(new FetchSourceContext(false));
        // 不获取已排序字段
        request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        return exists;
    }

    /**
     * 获取文档
     *
     * @throws IOException
     */
    public void getDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        GetRequest request = new GetRequest("user", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String sourceString = response.getSourceAsString();
        System.out.println(sourceString);
    }

    /**
     * 更新文档
     *
     * @throws IOException
     */
    public void updateDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        UpdateRequest request = new UpdateRequest("user", "1");
        User user = new User();
        user.setAge(31);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 根据查询条件批量更新
     *
     * @throws IOException
     */
    public void updateDocument2() throws IOException {
        UpdateByQueryRequest request = new UpdateByQueryRequest("user");
        // 设置查询条件
        request.setQuery(new TermQueryBuilder("name.keyword", "张三"));
        // 设置一次可以批处理的文档数，默认1000
        request.setBatchSize(200);
        // 更新后刷新索引
        request.setRefresh(true);
        // 通过脚本设置如何更新
        request.setScript(new Script("ctx._source.school = '复旦'"));
        BulkByScrollResponse response = client.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println("修改的文档数：" + response.getStatus().getUpdated());
    }

    /**
     * 删除文档
     *
     * @throws IOException
     */
    public void deleteDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        DeleteRequest request = new DeleteRequest("user", "1");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    /**
     * 根据查询条件批量删除
     *
     * @throws IOException
     */
    public void deleteDocument2() throws IOException {
        DeleteByQueryRequest request = new DeleteByQueryRequest("user");
        // 设置查询条件，查询school是复旦的
        request.setQuery(new TermQueryBuilder("school.keyword", "复旦"));
        // 设置一次可以批处理的文档数，默认1000
        request.setBatchSize(200);
        // 更新后刷新索引
        request.setRefresh(true);
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        System.out.println("删除的文档数：" + response.getStatus().getDeleted());
    }

    /**
     * 文档查询
     *
     * @throws IOException
     */
    public void searchDocument() throws IOException {
        SearchRequest request = new SearchRequest("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // school 是清华或北大的
        BoolQueryBuilder schoolQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("school.keyword", "北大"))
                .should(QueryBuilders.termQuery("school.keyword", "清华"));

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(schoolQueryBuilder)
                // name 以王开头的
                .must(QueryBuilders.matchPhrasePrefixQuery("name", "王"))
                // age 大于等于10小于等于70
                .must(QueryBuilders.rangeQuery("age").gte(10).lte(70));
        // 设置查询条件
        searchSourceBuilder.query(boolQueryBuilder);
        // 字段过滤
        String[] includeFields = new String[]{"name", "age", "school"};
        searchSourceBuilder.fetchSource(includeFields, new String[]{});
        // 设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);
        // 排序
        searchSourceBuilder.sort("age", SortOrder.DESC);
        // 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(20);
        // 超时时间
        searchSourceBuilder.timeout(new TimeValue(10, TimeUnit.SECONDS));
        request.source(searchSourceBuilder);
        // 发起查询请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits()) {
            // 提取高亮的字段内容，因为查询出来的文档数据和高亮字段的数据是分开的
            String highlightName = hit.getHighlightFields().get("name").fragments()[0].toString();
            // 提取查询出的文档数据，并转成对象
            User user = JSONObject.parseObject(hit.getSourceAsString(), User.class);
            // 用高亮的字段内容覆盖覆盖原文档字段
            user.setName(highlightName);
            System.out.println(JSON.toJSONString(user));
        }
    }
}
