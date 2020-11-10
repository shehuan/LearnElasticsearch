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
//        parseHtmlService.parse("Java", 30);
//        parseHtmlService.parse("Android开发", 30);
//        parseHtmlService.parse("iOS开发", 30);
//        parseHtmlService.parse("Python", 30);
//        parseHtmlService.parse("前端", 30);
//        parseHtmlService.parse("小说", 50);
//        parseHtmlService.parse("法律", 50);
//        parseHtmlService.parse("心理学", 30);
//        parseHtmlService.parse("医学", 30);
//        parseHtmlService.parse("哲学", 30);
//        parseHtmlService.parse("诗歌", 30);

//        fileService.writeFileDataToES();

        bookService.searchBook("Android", 0);
    }

}
