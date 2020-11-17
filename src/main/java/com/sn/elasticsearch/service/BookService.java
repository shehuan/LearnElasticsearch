package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.sn.elasticsearch.bean.Book;
import com.sn.elasticsearch.repository.BookRepository;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
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
                .must(QueryBuilders.rangeQuery("commentCount").gte(100));

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

        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("avg_price").field("price");
        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
        aggregations.add(priceAvgAggregation);
        nativeSearchQuery.setAggregations(aggregations);

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);
        long totalHits = search.getTotalHits();
        long totalPage = (totalHits % pageSize == 0) ? totalHits / pageSize : totalHits / pageSize + 1;

        System.out.println("命中数据条数：" + search.getTotalHits());
        System.out.println("总页数：" + totalPage);

        double avgPrice = ((Avg) search.getAggregations().get("avg_price")).getValue();
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
                .must(QueryBuilders.rangeQuery("commentCount").gte(100));

        HighlightBuilder.Field hbf1 = new HighlightBuilder.Field("author")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                .fragmentSize(10000)
                .numOfFragments(0);

        HighlightBuilder.Field hbf2 = new HighlightBuilder.Field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                .fragmentSize(10000)
                .numOfFragments(0);

        HighlightBuilder.Field[] hbfArray = new HighlightBuilder.Field[]{hbf1, hbf2};

        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("author").field("name")
                .preTags("<p>", "<span style='color:red'>")
                .postTags("</p>", "</span>")
                // 如果要高亮显示的字段内容很多,需要如下配置,避免高亮显示不全、内容缺失
                .fragmentSize(1000) // 最大高亮分片数
                .numOfFragments(0);// 从第一个分片获取高亮片段

        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withPageable(PageRequest.of(pageNum, pageSize))
                .withFields("author", "name", "price", "commentCount")
                .withSort(new FieldSortBuilder("price").order(SortOrder.ASC))
//                .withHighlightFields(hbfArray)
                .withHighlightBuilder(highlightBuilder)
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
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("author.keyword", ""));
        NativeSearchQuery nativeSearchQuery = new NativeSearchQuery(boolQueryBuilder);
        // 根据作者姓名进行分组统计，统计出的别名叫group_author
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("group_author").field("author.keyword");
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_price").field("price");
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("max_price").field("price");
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("min_price").field("price");
        ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders.count("book_count").field("author.keyword");
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("comment_count").field("commentCount").addRange(100, 10000);
        termsAggregationBuilder.subAggregation(avgAggregationBuilder);
        termsAggregationBuilder.subAggregation(maxAggregationBuilder);
        termsAggregationBuilder.subAggregation(minAggregationBuilder);
        termsAggregationBuilder.subAggregation(valueCountAggregationBuilder);
        termsAggregationBuilder.subAggregation(rangeAggregationBuilder);
        termsAggregationBuilder.order(BucketOrder.aggregation("book_count", false)).size(1000);

        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
        aggregations.add(termsAggregationBuilder);
        nativeSearchQuery.setAggregations(aggregations);
        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);

        Terms terms = search.getAggregations().get("group_author");

        for (Terms.Bucket bucket : terms.getBuckets()) {
            Avg avg = bucket.getAggregations().get("avg_price");
            Max max = bucket.getAggregations().get("max_price");
            Min min = bucket.getAggregations().get("min_price");
            ValueCount count = bucket.getAggregations().get("book_count");
            Range range = bucket.getAggregations().get("comment_count");

            System.out.println("作者：" + bucket.getKeyAsString() + "\n" +
                    "作品数：" + bucket.getDocCount() + "\n" +
                    "均价：" + avg.getValue() + "\n" +
                    "最高价：" + max.getValue() + "\n" +
                    "最低价：" + min.getValue());
            System.out.println("--------------------------------------------");
        }
    }
}
