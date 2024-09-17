package com.dp.rocketdemo.config;

import com.dp.rocketdemo.config.RocketMQProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
@Service
public class RocketMQConsumer {

    private DefaultMQPushConsumer consumer;

    @Autowired
    private RocketMQProperties rocketMQProperties;

    @PostConstruct
    public void init() throws Exception {
        // 初始化消费者
        consumer = new DefaultMQPushConsumer(rocketMQProperties.getConsumer().getGroup());
        consumer.setNamesrvAddr(rocketMQProperties.getNameServer());
        consumer.subscribe(rocketMQProperties.getConsumer().getTopic(), "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info("Received message: {}", new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        log.info("RocketMQ consumer started.");
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
            log.info("RocketMQ consumer shutdown.");
        }
    }
}
