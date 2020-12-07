package com.sn.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.sn.elasticsearch.bean.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@Service
public class BookFileService {
    @Autowired
    BookService bookService;

    /**
     * 将去重后的数据写入 ES
     */
    public void writeBookDataToES() {
        String filePath = System.getProperty("user.dir") + File.separator + "jd_book2.txt";

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

    /**
     * 根据 skuId 将采集到的原始数据去重
     */
    public void removeSameBookData() {
        // 原始数据文件路径
        String filePath = System.getProperty("user.dir") + File.separator + "jd_book.txt";
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        PrintStream printStream = null;
        try {
            Set<String> skuIdSet = new HashSet<>();
            // 创建去重后的文件
            File file = new File(System.getProperty("user.dir"), "jd_book2.txt");
            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();
            printStream = new PrintStream(new FileOutputStream(file));

            fileReader = new FileReader(filePath);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                Book book = JSON.parseObject(line, Book.class);
                if (!skuIdSet.contains(book.getSkuId())) {
                    skuIdSet.add(book.getSkuId());
                    // 将不重复的数写入新文件
                    printStream.println(line);
                }
            }
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
