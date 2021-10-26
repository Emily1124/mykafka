package pri.zyw.kafkademo.producer.service.Impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pri.zyw.kafkademo.consumer.config.KafkaConsumerConfig;
import pri.zyw.kafkademo.producer.config.KafkaProducerConfig;
import pri.zyw.kafkademo.producer.service.ProducerService;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class ProducerServiceImpl implements ProducerService {
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final String TOPIC3 = "topic3";

    //默认使用异步提交
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private KafkaProducerConfig producerConfig;

    @Autowired
    private KafkaConsumerConfig consumerConfig;

    /**
     * 同步发送消息
     *
     * @param msg
     */
    @Override
    public void sendMessageBySyn(String msg) {
        for (int i = 0; i < 15; i++) {
            try {
                kafkaTemplate.send(TOPIC3, 1, System.currentTimeMillis(), null, "testSyn-" + msg + " " + i)
                        .get(1000, TimeUnit.MILLISECONDS);
                // 发送带有时间戳的消息,此处的时间戳并不包含在消息中
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 异步发送消息
     *
     * @param msg
     */
    @Override
    public void sendMessageByAsy(String msg) {
        for (int i = 0; i < 15; i++) {
            kafkaTemplate.send(TOPIC2, 1, null, "testAsy-topic2-p1" + msg + i);
            kafkaTemplate.send(TOPIC2, 2, null, "testAsy-topic2-p2" + msg + i);
        }

    }

    /**
     * 测试事务声明
     */
    @Override
    public void testTransaction() {
        kafkaTemplate.executeInTransaction(kafkaOperations -> {   //本地事务，不用开启事务管理
            kafkaOperations.send(TOPIC3, "测试事务-topic1");
            throw new RuntimeException("超时");
        });

    }

    /**
     * 测试注解开启事务
     */
    @Transactional(rollbackFor = RuntimeException.class)
    public void annTransaction() {
        kafkaTemplate.send(TOPIC3, 1, null, "测试事务-topic3-p1");
        kafkaTemplate.send(TOPIC2, 2, null, "测试事务-topic2-p2");

        throw new RuntimeException("fail");
    }

    /**
     * 测试回调函数
     */
    @Override
    public void testCallBack() {
        kafkaTemplate.send(TOPIC2, "test producer listen");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 如果我指定一个不存在的topic,结果是还是能正常发送 不会返回发送失败

    }

    /**
     * 生产消费并存模式
     */
    @Override
    public void consumeTransferProduce() {
        KafkaProducer producer = new KafkaProducer(producerConfig.producerProps());
        KafkaConsumer consumer = consumerConfig.buildConsumer();
        //订阅主题
        consumer.subscribe(Arrays.asList("topic3"));
        Map offset = new HashMap<>();
        producer.initTransactions();
        while (true) {
            //拉取消息
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(5000));
            producer.beginTransaction();
            try {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    // 读取消息,并处理消息。
                    // 这里作为示例，只是简单的将消息打印出来而已
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
                    // 记录提交的消费的偏移量
                    offset.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset()));
                    producer.send(new ProducerRecord<String, String>("topic2", record.value()+"已消费"));
                }
                // 提交偏移量，
                // 其实这里本质上是将消费者消费的offset信息发送到名为__consumer-offset中的topic中
                producer.sendOffsetsToTransaction(offset, "group0");
                // 事务提交
                producer.commitTransaction();
            } catch (Exception e) {
                //中止事务
                producer.abortTransaction();
            }
        }
    }


}
