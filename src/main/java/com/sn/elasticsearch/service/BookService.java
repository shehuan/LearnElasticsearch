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
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
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
        // 指定文档的数据实体类
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
     * 批量添加数据
     */
    public void bulkAddBook(List<Book> books) {
        List<IndexQuery> indexQueryList = new ArrayList<>();
        books.forEach(book -> {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setObject(book);
            indexQueryList.add(indexQuery);
        });

        elasticsearchRestTemplate.bulkIndex(indexQueryList, IndexCoordinates.of("book"));
    }

    /**
     * 批量添加数据
     */
    public void bulkAddBook2(List<String> books) {
        List<IndexQuery> indexQueryList = new ArrayList<>();
        books.forEach(book -> {
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setSource(book);
            indexQueryList.add(indexQuery);
        });

        elasticsearchRestTemplate.bulkIndex(indexQueryList, IndexCoordinates.of("book"));
    }

    /**
     * 根据输入的关键字查询书籍
     *
     * @param keyword
     * @param pageNum  从1开始
     * @param pageSize
     */
    public List<Book> queryBook(String keyword, int pageNum, int pageSize) {

        BoolQueryBuilder keywordQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("author", keyword))
                .should(QueryBuilders.matchPhraseQuery("name", keyword));

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

        List<Book> resultList = new ArrayList<>();
        for (SearchHit<Book> searchHit : search.getSearchHits()) {
            Book book = searchHit.getContent();
            if (searchHit.getHighlightFields().containsKey("author")) {
                // 提取高亮字段
                book.setAuthor(searchHit.getHighlightFields().get("author").get(0));
            }
            if (searchHit.getHighlightFields().containsKey("name")) {
                // 提取高亮字段
                book.setName(searchHit.getHighlightFields().get("name").get(0));
            }
            resultList.add(book);
            System.out.println(JSONObject.toJSONString(book));
        }

        return resultList;
    }

    public void queryBook2(String keyword, int pageNum, int pageSize) {

        BoolQueryBuilder keywordQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("author", keyword))
                .should(QueryBuilders.matchPhraseQuery("name", keyword));

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

    /**
     * 根据文档id查询
     */
    public Book queryBookById(String id) {
        Book book = elasticsearchRestTemplate.get(id, Book.class);
        System.out.println(JSONObject.toJSONString(book));
        return book;
    }

    /**
     * 根据文档id删除
     */
    public void deleteBookById(String id) {
        String result = elasticsearchRestTemplate.delete(id, Book.class);
        System.out.println(result);
    }

    /**
     * 自定义删除条件，例如根据skuId删除
     */
    public void deleteBookBySkuId(String skuId) {
        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.termQuery("skuId", skuId))
                .build();
        elasticsearchRestTemplate.delete(nativeSearchQuery, Book.class, IndexCoordinates.of("book"));
    }

    /**
     * 根据文档id修改
     */
    public void updateBookById(String id) {
        Document document = Document.create();
        document.put("commentCount", 1214666);
        document.put("price", 66.6);
        UpdateQuery updateQuery = UpdateQuery.builder(id).withDocument(document).build();
        UpdateResponse response = elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of("book"));
        System.out.println(response.getResult().name());
    }

    /**
     * 根据多个文档id批量修改
     */
    public void bulkUpdateBook(String... ids) {
        List<UpdateQuery> updateQueryList = new ArrayList<>();
        for (String id : ids) {
            Document document = Document.create();
            document.put("commentCount", 1214666);
            document.put("price", 66.6);
            UpdateQuery updateQuery = UpdateQuery.builder(id).withDocument(document).build();
            updateQueryList.add(updateQuery);
        }
        elasticsearchRestTemplate.bulkUpdate(updateQueryList, IndexCoordinates.of("book"));
    }
}
