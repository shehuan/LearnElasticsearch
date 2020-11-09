package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.sn.elasticsearch.bean.User;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@Service
public class ESApiService {
    ElasticsearchRestTemplate template;

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    public void createIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("user");
        CreateIndexResponse create = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(create);
    }

    public boolean existsIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("user");
        return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    }

    public void deleteIndex() throws IOException {
        if (!existsIndex()) {
            return;
        }
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("user");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    public void addDocument() throws IOException {
        User user = new User("张三", 12);
        IndexRequest request = new IndexRequest("user");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    public boolean existsDocument() throws IOException {
        GetRequest getRequest = new GetRequest("user", "1");
        // 不获取返回的_source
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    public void getDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        GetRequest getRequest = new GetRequest("user", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }

    public void updateDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        UpdateRequest updateRequest = new UpdateRequest("user", "1");
        User user = new User("张三", 14);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    public void deleteDocument() throws IOException {
        if (!existsDocument()) {
            return;
        }
        DeleteRequest deleteRequest = new DeleteRequest("user", "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    public void bulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        for (int i = 0; i < 100; i++) {
            User user = new User("张三" + i, i);
            bulkRequest.add(new IndexRequest("user").id("" + (i + 1)).source(JSON.toJSONString(user), XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    public void searchDocument() throws IOException {
        SearchRequest searchRequest = new SearchRequest("user");
        // trackTotalHits，突破最大查询10000条的限制
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().trackTotalHits(true);
        searchSourceBuilder.highlighter(new HighlightBuilder().field("name").preTags("<span style='color:red'>").postTags("</span>"));
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "张三66");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(new MatchQueryBuilder("name", "张三"))
//                .must(new TermQueryBuilder("name.keyword", "张三18"))
                .filter(new RangeQueryBuilder("age").gte(10).lte(30));
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(50);
        searchSourceBuilder.timeout(new TimeValue(10, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit hit : searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }
}
