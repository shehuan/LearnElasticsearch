package com.sn.elasticsearch;

import com.sn.elasticsearch.service.BookService;
import com.sn.elasticsearch.service.UserService;
import com.sn.elasticsearch.service.FileService;
import com.sn.elasticsearch.service.ParseHtmlService;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
class LearnElasticsearchApplicationTests {

    @Autowired
    UserService userService;

    @Autowired
    ParseHtmlService parseHtmlService;

    @Autowired
    FileService fileService;

    @Autowired
    BookService bookService;

    @Test
    void testES() throws IOException {
//        userService.createIndex2();
//        userService.deleteIndex();
//        userService.addDocument();
//        userService.bulkAddDocument();
//        userService.getDocument();
//        userService.updateDocument2();
//        userService.deleteDocument2();
//        userService.searchDocument();
//        userService.avg();
//        userService.max();
//        userService.range();
//        userService.valueCount();
//        userService.terms();
        userService.topHits();

//        parseHtmlService.parse("小说", 400);
//        parseHtmlService.parse("Java开发", 30);
//        parseHtmlService.parse("Android开发", 20);
//        parseHtmlService.parse("iOS开发", 20);
//        parseHtmlService.parse("Python开发", 20);
//        parseHtmlService.parse("前端开发", 20);
//        parseHtmlService.parse("诗词", 30);
//        parseHtmlService.parse("法律", 30);
//        parseHtmlService.parse("军事", 30);
//        parseHtmlService.parse("经济", 30);
//        parseHtmlService.parse("历史", 30);

//        fileService.removeSameData();

//        fileService.writeFileDataToES();

//        bookService.queryBook("刘慈欣", 0, 20);

//        bookService.aggregation();
    }

}
