package com.priyanka.datta.springkafkaasync.service;

import com.priyanka.datta.springkafkaasync.entity.Book;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.stream.IntStream;

@Component
public class ProducerService {

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMessage(Book book){
        IntStream.range(0,2).forEach(i->{
            ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send("test", String.valueOf(i), book);

            listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                @Override
                public void onFailure(Throwable ex) {
                    logger.error("Error occurred {}", ex.getMessage());
                }

                @Override
                public void onSuccess(SendResult<String, Object> result) {
                    logger.info("Topic : {}", result.getRecordMetadata().topic());
                    logger.info("partition : {}", result.getRecordMetadata().partition());
                    logger.info("Offset : {}", result.getRecordMetadata().offset());
                    logger.info("timestamp : {}", result.getRecordMetadata().timestamp());
                    logger.info("Message key size : {}", result.getRecordMetadata().serializedKeySize());
                }
            });
        });

    }

    public void sendMessageWithKafkaProducer(Book book){
        ProducerRecord<String,Object> producerRecord = new ProducerRecord<>("test",book);
        ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                logger.error("Error occurred {}", ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                logger.info("Topic : {}",result.getRecordMetadata().topic());
                logger.info("partition : {}",result.getRecordMetadata().partition());
                logger.info("Offset : {}",result.getRecordMetadata().offset());
                logger.info("timestamp : {}",result.getRecordMetadata().timestamp());
                logger.info("Message key size : {}",result.getRecordMetadata().serializedKeySize());
            }
        });
    }
}
