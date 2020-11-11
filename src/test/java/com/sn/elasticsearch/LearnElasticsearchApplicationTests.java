package com.sn.elasticsearch;

import com.sn.elasticsearch.service.BookService;
import com.sn.elasticsearch.service.ESApiService;
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
    ESApiService esApiService;

    @Autowired
    ParseHtmlService parseHtmlService;

    @Autowired
    FileService fileService;

    @Autowired
    BookService bookService;

    @Test
    void contextLoads() throws IOException {
//        esApiService.createIndex();
//        esApiService.deleteIndex();
//        esApiService.addDocument();
//        esApiService.getDocument();
//        esApiService.updateDocument();
//        esApiService.bulkAddDocument();
//        esApiService.searchDocument();

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

        bookService.searchBook("艺术探索", 0);
    }

}
