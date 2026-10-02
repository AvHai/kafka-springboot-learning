package com.example.kafkaspringboot.dto;

import lombok.Data;

@Data
public class MessageRequest {
    private String messageContent;
    private String sender;
}
