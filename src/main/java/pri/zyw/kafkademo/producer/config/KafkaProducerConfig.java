package pri.zyw.kafkademo.producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义kafka配置，默认配置无法满足日常需求
 */
@Configuration
public class KafkaProducerConfig {

    public Map<String, Object> producerProps(){
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 1); // 重试次数大于0，则客户端会发送失败的记录重新发送
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 应答级别:多少个分区副本备份完成时向生产者发送ack确认(可选0、1、all/-1)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true); //开启生产者幂等性
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"test-transactional"); // 设置transactionalId
        //使用自定义kafka拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"pri.zyw.kafkademo.producer.interceptor.TimeInterceptor");

        return props;
    }

    @Bean
    public ProducerFactory<String,Object> producerFactory(){
        DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory(producerProps());
        //factory.transactionCapable();
        factory.setTransactionIdPrefix("trans");
        return factory;
    }

    @Bean
    public KafkaTemplate kafkaTemplate(){
        return new KafkaTemplate(producerFactory());
    }

    @Bean
    public KafkaTransactionManager transactionManager(){
        KafkaTransactionManager manager = new KafkaTransactionManager(producerFactory());
        return manager;
    }


}
