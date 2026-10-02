package com.example.kafkaspringboot.service;

import com.example.kafkaspringboot.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaRealTimeConsumerService {
    @KafkaListener(
            topics = "driver-location",
            groupId = "demo-group"
    )
    public void listen(
            @Payload Message message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ){
      log.info("Received message from topic: {}, partition: {}, offset: {}", topic, partition, offset);
      log.info("message content: {}", message);

      try{
          processRealTimeNotification(message);
          log.info("Message processed successfully");
      } catch (Exception e) {
          log.error("Message processing failed with : {}",e.getMessage());
          throw new RuntimeException(e);
      }
    }
    public void processRealTimeNotification(Message message){
        log.info("Processing real-time notification : {}",message.getMessageContent());
        /**
         * Business Logic will be added here
         */
        try {
                Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
