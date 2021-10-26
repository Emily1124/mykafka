package pri.zyw.kafkademo.producer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import pri.zyw.kafkademo.producer.service.ProducerService;


/**
 * 生产者
 */
@RestController
public class ProducerController {

    @Autowired
    private ProducerService controllerService;

    /**
     * 异步处理
     *
     * @param msg
     */
    @GetMapping("/kafka/asy/{msg}")
    @Transactional
    public void sendByAsy(@PathVariable("msg") String msg) {
        controllerService.sendMessageByAsy(msg);
    }

    /**
     * 同步处理
     *
     * @param msg
     */
    @GetMapping("/kafka/syn/{msg}")
    @Transactional
    public void sendBySyn(@PathVariable("msg") String msg) {
        controllerService.sendMessageBySyn(msg);
    }

    /**
     * 回调函数
     */
    @GetMapping("/kafka/callback")
    @Transactional
    public void kafkaCallback() {
        controllerService.testCallBack();
    }

    /**
     * 本地事务处理
     */
    @GetMapping("/kafka/transaction")
    @Transactional
    public void kafkaTransaction() {
        controllerService.testTransaction();
    }

    /**
     * 使用注解开启事务
     */
    @GetMapping("/kafka/transaction2")
    @Transactional(rollbackFor = RuntimeException.class)
    public void kafkaTransaction2() throws InterruptedException {
        controllerService.annTransaction();
    }


    @GetMapping("/kafka/CTP")
    public void CTP(){
        controllerService.consumeTransferProduce();
    }



}
