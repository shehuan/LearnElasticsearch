package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.sn.elasticsearch.bean.Book;
import com.sn.elasticsearch.repository.BookRepository;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class BookService {
    @Autowired
    BookRepository bookRepository;

    @Autowired
    ElasticsearchRestTemplate elasticsearchRestTemplate;

    public void addBook(List<Book> books) {
        elasticsearchRestTemplate.save(books);
    }

    public void queryBook(String keyword, int pageNum, int pageSize) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("author", keyword))
                        .should(QueryBuilders.matchPhraseQuery("name", keyword)))
//                boolQueryBuilder.must(new MatchPhraseQueryBuilder("name", keyword))
                .must(QueryBuilders.rangeQuery("commentCount").gte(1000));

        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("author").field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                // 如果要高亮显示的字段内容很多,需要如下配置,避免高亮显示不全、内容缺失
                .fragmentSize(1000) // 最大高亮分片数
                .numOfFragments(0);// 从第一个分片获取高亮片段
        HighlightQuery highlightQuery = new HighlightQuery(highlightBuilder);

        NativeSearchQuery nativeSearchQuery = new NativeSearchQuery(boolQueryBuilder);
        nativeSearchQuery.setPageable(PageRequest.of(pageNum, pageSize));
        nativeSearchQuery.addFields("name", "price", "commentCount", "author");
        nativeSearchQuery.addSort(Sort.by("price").ascending());
        nativeSearchQuery.setHighlightQuery(highlightQuery);

        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("avgPrice").field("price");
        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
        aggregations.add(priceAvgAggregation);
        nativeSearchQuery.setAggregations(aggregations);

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);
        long totalHits = search.getTotalHits();
        long totalPage = (totalHits % pageSize == 0) ? totalHits / pageSize : totalHits / pageSize + 1;

        System.out.println("命中数据条数：" + search.getTotalHits());
        System.out.println("总页数：" + totalPage);

        double avgPrice = ((ParsedAvg) search.getAggregations().asMap().get("avgPrice")).getValue();
        System.out.println("搜索到的书籍均价：" + avgPrice);

        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            if (searchHit.getHighlightFields().containsKey("author")) {
                searchHit.getContent().setAuthor(searchHit.getHighlightFields().get("author").get(0));
            }

            if (searchHit.getHighlightFields().containsKey("name")) {
                searchHit.getContent().setName(searchHit.getHighlightFields().get("name").get(0));
            }

            System.out.println(JSONObject.toJSONString(searchHit.getContent()));
        }
    }

    public void queryBook2(String keyword, int pageNum, int pageSize) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery().should(QueryBuilders.matchPhraseQuery("author", keyword))
                        .should(QueryBuilders.matchPhraseQuery("name", keyword)))
//                boolQueryBuilder.must(new MatchPhraseQueryBuilder("name", keyword))
                .must(QueryBuilders.rangeQuery("commentCount").gte(1000));

        HighlightBuilder.Field hbf1 = new HighlightBuilder.Field("author")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                .fragmentSize(10)
                .numOfFragments(0);

        HighlightBuilder.Field hbf2 = new HighlightBuilder.Field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                .fragmentSize(10)
                .numOfFragments(0);

        HighlightBuilder.Field[] hbfArray = new HighlightBuilder.Field[]{hbf1, hbf2};

        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withPageable(PageRequest.of(pageNum, pageSize))
                .withFields("author", "name", "price", "commentCount")
                .withSort(new FieldSortBuilder("price").order(SortOrder.ASC))
                .withHighlightFields(hbfArray)
                .build();

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);

        long totalHits = search.getTotalHits();
        long totalPage = (totalHits % pageSize == 0) ? totalHits / pageSize : totalHits / pageSize + 1;

        System.out.println("命中数据条数：" + search.getTotalHits());
        System.out.println("总页数：" + totalPage);

        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            if (searchHit.getHighlightFields().containsKey("author")) {
                searchHit.getContent().setAuthor(searchHit.getHighlightFields().get("author").get(0));
            }

            if (searchHit.getHighlightFields().containsKey("name")) {
                searchHit.getContent().setName(searchHit.getHighlightFields().get("name").get(0));
            }

            System.out.println(JSONObject.toJSONString(searchHit.getContent()));
        }
    }

    public void aggregation() {
//        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("price2").field("price");
//        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
//        aggregations.add(priceAvgAggregation);
//        nativeSearchQuery.setAggregations(aggregations);
    }
}
