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
public class FileService {
    @Autowired
    BookService bookService;

    public void writeFileDataToES() {
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

    public void removeSameData() {
        String filePath = System.getProperty("user.dir") + File.separator + "jd_book.txt";

        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        PrintStream printStream = null;
        int i = 0;
        try {

            Set<String> skuIdSet = new HashSet<>();

            File file = new File(System.getProperty("user.dir"), "jd_book2.txt");
            if (file.exists()){
                file.delete();
            }
            file.createNewFile();

            printStream = new PrintStream(new FileOutputStream(file));

            fileReader = new FileReader(filePath);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                ++i;
                Book book = JSON.parseObject(line, Book.class);
                if (!skuIdSet.contains(book.getSkuId())) {
                    skuIdSet.add(book.getSkuId());
                    printStream.println(line);
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(i);
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
