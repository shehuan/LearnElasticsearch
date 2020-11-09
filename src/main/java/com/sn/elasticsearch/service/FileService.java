package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.sn.elasticsearch.bean.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;

@Service
public class FileService {
    @Autowired
    BookService bookService;

    public void writeFileDataToES() {
        String filePath = System.getProperty("user.dir") + File.separator + "jd_book.txt";

        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader(filePath);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            ArrayList<Book> books = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                books.add(JSON.parseObject(line, Book.class));
                if (books.size() >= 500) {
                    bookService.addBook(books);
                    books.clear();
                }
            }
            bookService.addBook(books);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
