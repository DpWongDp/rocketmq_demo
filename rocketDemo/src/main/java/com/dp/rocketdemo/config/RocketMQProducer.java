package com.dp.rocketdemo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
@Slf4j
public class RocketMQProducer {

    private DefaultMQProducer producer;

    @Autowired
    private RocketMQProperties rocketMQProperties;

    @PostConstruct
    public void start() throws Exception {
        // 初始化生产者
        producer = new DefaultMQProducer(rocketMQProperties.getProducer().getGroup());
        producer.setNamesrvAddr(rocketMQProperties.getNameServer());
        producer.start();
        log.info("RocketMQ producer started.");
    }

    public void sendMessage(String messageText) throws Exception {
        String topic = rocketMQProperties.getProducer().getTopic();
        Message message = new Message(topic, messageText.getBytes());
        SendResult sendResult = producer.send(message);
        log.info("Message sent: {} SendResult: {}", messageText, sendResult);
    }

    public void sendDelayedMessage(String messageText, long delayMillis) throws Exception {
        String topic = rocketMQProperties.getProducer().getTopic();
        Message message = new Message(topic, messageText.getBytes());

        // 设置自定义延迟时间（毫秒）
        message.setDelayTimeMs(delayMillis);

        SendResult sendResult = producer.send(message);
        log.info("Delayed message sent: {} with delayMillis: {} SendResult: {}", messageText, delayMillis, sendResult);
    }

    @PreDestroy
    public void shutdown() {
        if (producer != null) {
            producer.shutdown();
            log.info("RocketMQ producer shutdown.");
        }
    }
}
