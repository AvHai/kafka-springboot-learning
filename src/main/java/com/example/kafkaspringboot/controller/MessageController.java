package com.example.kafkaspringboot.controller;

import com.example.kafkaspringboot.dto.MessageRequest;
import com.example.kafkaspringboot.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/messages")
@RequiredArgsConstructor
public class MessageController {
    private final KafkaProducerService kafkaProducerService;

    @GetMapping("/checkhealth")
    public ResponseEntity<String> checkHealth(){
        return ResponseEntity.ok().body("Kafka Server is up and running");
    }

    @PostMapping("/sendmessage")
    public ResponseEntity<String> sendMessage(@RequestBody MessageRequest  messageRequest){
        String messageId = UUID.randomUUID().toString();
        kafkaProducerService.sendMessage(messageId,messageRequest.getMessageContent(),messageRequest.getSender());
        return ResponseEntity.ok().body("Message send with ID " + messageId);
    }
}
