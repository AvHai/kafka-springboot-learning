package com.example.kafkaspringboot.service;

import com.example.kafkaspringboot.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    private static final String TOPIC = "driver-location";

    private final KafkaTemplate<String,Object> kafkaTemplate;

    public void messageRequest(Message  message){
        log.info("Sending message to the TOPIC: {}",TOPIC);

        CompletableFuture <SendResult<String,Object>> future = kafkaTemplate.send(TOPIC, message.getId(), message);

        future.whenComplete((result,exception)->{
            if(exception!=null){
                log.error("Error while sending the message to the TOPIC: {} with exception : {} ",TOPIC,exception.getMessage());
            }
            else{
                log.info("Message sent successfully to the TOPIC: {} with the offeset : {}",TOPIC, result.getRecordMetadata().offset());
            }
        });
    }
    public void sendMessage(
            String messageId,
            String messageContent,
            String sender
    ) {
        Message message = new Message(messageId, messageContent, sender);
        messageRequest(message);
    }
}
