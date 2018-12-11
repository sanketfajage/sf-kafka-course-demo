package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

    private CountDownLatch countDownLatch;
    KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        //create consumer
        consumer = new KafkaConsumer<>(getConsumerProperties());
        //subscribe to topic list
        consumer.subscribe(Arrays.asList("first_topic"));
    }

    @Override
    public void run() {
        try {
            while (true) {
                //poll for data
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Topic: " + record.topic() + "\tPartition: " + record.partition() + "\tOffset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            countDownLatch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhot:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}