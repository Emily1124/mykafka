package pri.zyw.kafkademo.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class TestConsumer {

    //订阅模式
    @KafkaListener(topics = "topic2",groupId = "group1")
    public void listenGroup1(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment ack){
        System.out.println("收到"+ consumerRecords.size() + "条消息：");
        for(ConsumerRecord<String, String> record : consumerRecords) {
            System.out.println("topic:"+record.topic()+"- partition:"+record.partition()+"- value:"+record.value()+"- offset:"+record.offset());
        }
        System.out.println("批量消费结束");
        ack.acknowledge();
    }




}
