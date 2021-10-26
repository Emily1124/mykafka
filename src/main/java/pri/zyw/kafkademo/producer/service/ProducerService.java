package pri.zyw.kafkademo.producer.service;

public interface ProducerService {

    //同步发送消息
    void sendMessageBySyn(String msg);

    //异步发送消息
    void sendMessageByAsy(String msg);

    //测试事务
    void testTransaction();

    //测试注解事务
    void annTransaction();

    //测试回调函数
    void testCallBack();

    //消费-生产并存
    void consumeTransferProduce();

}
