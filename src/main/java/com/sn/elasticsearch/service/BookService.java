package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.sn.elasticsearch.bean.Book;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.query.HighlightQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class BookService {
//    @Autowired
//    BookRepository bookRepository;


    @Autowired
    ElasticsearchRestTemplate elasticsearchRestTemplate;

    /**
     * 创建索引
     */
    public void createIndex() {
        // 和数据实体类绑定
        IndexOperations indexOperations = elasticsearchRestTemplate.indexOps(Book.class);
        // 创建索引
        indexOperations.create();
        // 创建字段映射
        Document mapping = indexOperations.createMapping();
        // 给索引设置字段映射
        indexOperations.putMapping(mapping);
    }

    /**
     * 添加数据
     *
     * @param books
     */
    public void addBook(List<Book> books) {
        elasticsearchRestTemplate.save(books);
    }

    /**
     * 根据输入的关键字查询书籍
     *
     * @param keyword
     * @param pageNum  从1开始
     * @param pageSize
     */
    public void queryBook(String keyword, int pageNum, int pageSize) {

        BoolQueryBuilder keywordQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("author", keyword).analyzer("ik_smart"))
                .should(QueryBuilders.matchPhraseQuery("name", keyword).analyzer("ik_smart"));

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(keywordQueryBuilder)
                .must(QueryBuilders.rangeQuery("commentCount").gte(10));

//        HighlightBuilder.Field hbf1 = new HighlightBuilder.Field("author")
//                .preTags("<span style='color:red'>")
//                .postTags("</span>")
//                .fragmentSize(10000)
//                .numOfFragments(0);
//
//        HighlightBuilder.Field hbf2 = new HighlightBuilder.Field("name")
//                .preTags("<span style='color:green'>")
//                .postTags("</span>")
//                .fragmentSize(10000)
//                .numOfFragments(0);

//        HighlightBuilder.Field[] hbfArray = new HighlightBuilder.Field[]{hbf1, hbf2};

        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("author").field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                // 如果要高亮显示的字段内容很多,需要如下配置,避免高亮显示不全、内容缺失
                .fragmentSize(1000) // 最大高亮分片数
                .numOfFragments(0);// 从第一个分片获取高亮片段

        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("avgPrice").field("price");

        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withPageable(PageRequest.of(pageNum - 1, pageSize))
                .withFields("author", "name", "price", "commentCount")
                .withSort(new FieldSortBuilder("price").order(SortOrder.ASC))
//                .withHighlightFields(hbfArray)
                .withHighlightBuilder(highlightBuilder)
                .addAggregation(priceAvgAggregation)
                .build();

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);

        long totalHits = search.getTotalHits();
        long totalPage = (totalHits % pageSize == 0) ? totalHits / pageSize : totalHits / pageSize + 1;

        System.out.println("总数据条数：" + search.getTotalHits());
        System.out.println("总页数：" + totalPage);
        System.out.println("当前页码：" + pageNum);

        double avgPrice = ((Avg) search.getAggregations().get("avgPrice")).getValue();
        System.out.println("搜索到的书籍均价：" + avgPrice);

        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            if (searchHit.getHighlightFields().containsKey("author")) {
                // 提取高亮字段
                searchHit.getContent().setAuthor(searchHit.getHighlightFields().get("author").get(0));
            }
            if (searchHit.getHighlightFields().containsKey("name")) {
                // 提取高亮字段
                searchHit.getContent().setName(searchHit.getHighlightFields().get("name").get(0));
            }
            System.out.println(JSONObject.toJSONString(searchHit.getContent()));
        }
    }

    public void queryBook2(String keyword, int pageNum, int pageSize) {

        BoolQueryBuilder keywordQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("author", keyword).analyzer("ik_smart"))
                .should(QueryBuilders.matchPhraseQuery("name", keyword).analyzer("ik_smart"));

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(keywordQueryBuilder)
                .must(QueryBuilders.rangeQuery("commentCount").gte(10));

        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("author").field("name")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                // 如果要高亮显示的字段内容很多,需要如下配置,避免高亮显示不全、内容缺失
                .fragmentSize(1000) // 最大高亮分片数
                .numOfFragments(0);// 从第一个分片获取高亮片段
        HighlightQuery highlightQuery = new HighlightQuery(highlightBuilder);

        NativeSearchQuery nativeSearchQuery = new NativeSearchQuery(boolQueryBuilder);
        nativeSearchQuery.setPageable(PageRequest.of(pageNum - 1, pageSize));
        nativeSearchQuery.addFields("name", "price", "commentCount", "author");
        nativeSearchQuery.addSort(Sort.by("price").ascending());
        nativeSearchQuery.setHighlightQuery(highlightQuery);

        AvgAggregationBuilder priceAvgAggregation = AggregationBuilders.avg("avgPrice").field("price");
        MinAggregationBuilder priceMinAggregation = AggregationBuilders.min("minPrice").field("price");
        List<AbstractAggregationBuilder> aggregations = new ArrayList<>();
        aggregations.add(priceAvgAggregation);
        aggregations.add(priceMinAggregation);
        nativeSearchQuery.setAggregations(aggregations);

        SearchHits<Book> search = elasticsearchRestTemplate.search(nativeSearchQuery, Book.class);
        long totalHits = search.getTotalHits();
        long totalPage = (totalHits % pageSize == 0) ? totalHits / pageSize : totalHits / pageSize + 1;

        System.out.println("总数据条数：" + search.getTotalHits());
        System.out.println("总页数：" + totalPage);

        double avgPrice = ((Avg) search.getAggregations().get("avgPrice")).getValue();
        double minPrice = ((Min) search.getAggregations().get("minPrice")).getValue();
        System.out.println("搜索到的书籍均价：" + avgPrice);
        System.out.println("搜索到的书籍最低价：" + minPrice);
        System.out.println("当前页码：" + pageNum);

        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            if (searchHit.getHighlightFields().containsKey("author")) {
                searchHit.getContent().setAuthor(searchHit.getHighlightFields().get("author").get(0));
            }

            if (searchHit.getHighlightFields().containsKey("name")) {
                searchHit.getContent().setName(searchHit.getHighlightFields().get("name").get(0));
            }

            System.out.println(JSONObject.toJSONString(searchHit.getContent()));
            System.out.println("\n");
        }
    }
}
