package demo;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private String consumerKey = "Uq2UzNz8DyUPODP1A3x2My8dL";
    private String consumerSecret = "ZLnBzsBT7eY2GEm2uHwZwQOvxavBYbcQz24ib8HpDTQ8DBfzAb";
    private String token = "855411544737415168-Ekmfk8ayCT8SqnTQsIZk7DKVxU7Cs2R";
    private String secret = "8z9for6uLxWrixOxCsBG1o7iL2yDXO1AOMzHnJOLH7QVz";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);

        //create twitter client
        Client client = createTwitterClient(msgQueue);
        //attempt to connect
        client.connect();

        //create twitter producer
        KafkaProducer<String, String> producer = createProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing application...");
            logger.info("Stopping twitter client...");
            client.stop();
            logger.info("Closing kafka producer...");
            producer.close();
            logger.info("Done!");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (StringUtils.isNotBlank(msg)) {
                    logger.info(msg);
                    producer.send(new ProducerRecord<>("tweeter_tweets_topic", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Exception while sending message to kafka topic is ", e);
                            }
                        }
                    });
                    System.out.println(msg);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted : " + e);
                client.stop();
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("modi");
        statusesFilterEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Simple-Client-1")                              // optional: mainly for the logs
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(statusesFilterEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createProducer() {
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //create high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //create producer
        return new KafkaProducer<>(properties);
    }
}
