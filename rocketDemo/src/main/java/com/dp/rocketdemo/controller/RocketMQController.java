package com.dp.rocketdemo.controller;

import com.dp.rocketdemo.config.RocketMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RocketMQController {

    @Autowired
    private RocketMQProducer rocketMQProducer;

    @GetMapping("/send")
    public String sendMessage(@RequestParam String message) {
        try {
            rocketMQProducer.sendMessage(message);
            return "Message sent successfully!";
        } catch (Exception e) {
            return "Failed to send message: " + e.getMessage();
        }
    }
}

