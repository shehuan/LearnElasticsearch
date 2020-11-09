package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.sn.elasticsearch.bean.Book;
import com.sn.elasticsearch.repository.BookRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
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

    public List<Book> searchBook() {
        return new ArrayList<>();
    }
}
