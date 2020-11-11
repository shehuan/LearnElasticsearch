package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.sn.elasticsearch.bean.Book;
import com.sn.elasticsearch.repository.BookRepository;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
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

    public void searchBook(String keyword, int pageNum) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders
                .boolQuery()
                .must(new MatchPhraseQueryBuilder("name", keyword))
                .must(new RangeQueryBuilder("commentCount").gte(1000));
        NativeSearchQuery nativeSearchQuery = new NativeSearchQuery(boolQueryBuilder);
        nativeSearchQuery.setPageable(PageRequest.of(pageNum, 20));
        nativeSearchQuery.addSort(Sort.by("price").ascending());
        nativeSearchQuery.setHighlightQuery(new HighlightQuery(new HighlightBuilder().field("name").preTags("<span style='color:red'>").postTags("</span>")));
        nativeSearchQuery.addFields("name", "price", "commentCount", "author");

        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("price2").field("price");
        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
        aggregations.add(priceAvgAggregation);
        nativeSearchQuery.setAggregations(aggregations);

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);
        System.out.println("总数据条数：" + search.getTotalHits());
        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            searchHit.getContent().setName(searchHit.getHighlightFields().get("name").get(0));
            System.out.println(JSONObject.toJSONString(searchHit.getContent()));
        }
    }
}
