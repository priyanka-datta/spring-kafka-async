package com.priyanka.datta.springkafkaasync.controller;

import com.priyanka.datta.springkafkaasync.entity.Book;
import com.priyanka.datta.springkafkaasync.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerControllerAsync {

    @Autowired
    private ProducerService producerService;

    @RequestMapping(value = "/book",method = RequestMethod.POST)
    public Book method(@RequestBody Book book){
        producerService.sendMessage(book);
        return book;
    }

    @RequestMapping(value = "/book/producerRecord",method = RequestMethod.POST)
    public Book method2(@RequestBody Book book){
        producerService.sendMessage(book);
        return book;
    }
}
