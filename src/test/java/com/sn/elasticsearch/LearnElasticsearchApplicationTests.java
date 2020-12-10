package com.sn.elasticsearch;

import com.sn.elasticsearch.service.BookService;
import com.sn.elasticsearch.service.UserService;
import com.sn.elasticsearch.service.BookFileService;
import com.sn.elasticsearch.service.BookParseService;
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
    BookParseService bookParseService;

    @Autowired
    BookFileService bookFileService;

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
//        userService.topHits();

//        bookParseService.parse("小说", 400);
//        bookParseService.parse("Java开发", 30);
//        bookParseService.parse("Android开发", 20);
//        bookParseService.parse("iOS开发", 20);
//        bookParseService.parse("Python开发", 20);
//        bookParseService.parse("前端开发", 20);
//        bookParseService.parse("诗词", 30);
//        bookParseService.parse("法律", 30);
//        bookParseService.parse("军事", 30);
//        bookParseService.parse("经济", 30);
//        bookParseService.parse("历史", 30);

//        bookService.createIndex();

//        bookFileService.removeSameBookData();
//        bookFileService.writeBookDataToES();

        bookService.queryBook("刘慈欣", 1, 10);
//        bookService.queryBook("唐家三少", 0, 10);
    }
}
