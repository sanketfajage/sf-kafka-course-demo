package elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

	private static JsonParser jsonParser = new JsonParser();

	private static String host = "host";
	private static String accessKey = "access-key";
	private static String accessSecret = "access-secret";

	public static void main(String[] args) {
		RestHighLevelClient restHighLevelClient = createRestHighLevelClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer("tweeter_tweets");

		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			if (consumerRecords.count() > 0) {
				System.out.println("Records count: " + consumerRecords.count());
				insertTweetsToElasticSearch(restHighLevelClient, consumerRecords);
				System.out.println("Committing offset...");
				consumer.commitSync();
				System.out.println("Offset committed...");
			}
		}
	}

	private static RestHighLevelClient createRestHighLevelClient() {
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(accessKey, accessSecret));

		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
						return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		return new RestHighLevelClient(restClientBuilder);
	}

	private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
		Properties properties = getConsumerProperties();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	private static Properties getConsumerProperties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhot:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_application");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		return properties;
	}

	private static void insertTweetsToElasticSearch(RestHighLevelClient restHighLevelClient,
			ConsumerRecords<String, String> consumerRecords) {
		BulkRequest bulkRequest = new BulkRequest();
		for (ConsumerRecord<String, String> record : consumerRecords) {
			try {
				String tweetId = extractTweetId(record.value());
				IndexRequest indexRequest = new IndexRequest("tweeter", "tweets").source(record.value(),
						XContentType.JSON, tweetId);
				bulkRequest.add(indexRequest);
			} catch (NullPointerException e) {
				logger.warn("Skipping bad data...");
			}
		}

		try {
			restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
			Thread.sleep(1000);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static String extractTweetId(String value) {
		return jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
	}
}
