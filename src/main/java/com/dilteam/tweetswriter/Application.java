package com.dilteam.tweetswriter;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

@ComponentScan
@EnableAutoConfiguration
@ConfigurationProperties
public class Application implements CommandLineRunner {

    @Autowired
    ProducerConfig producerConfig;

    @Autowired
    Producer<String, String> producer;

    private String authConsumerKey;
    private String authConsumerSecret;
    private String authAccessToken;
    private String authAccessTokenSecret;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class);
    }

    @Override
    public void run(String... args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Inside shutdown hook! Closing producer");
                producer.close();
                System.out.println("Inside shutdown hook! Producer closed");
            }
        });

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(getAuthConsumerKey())
                .setOAuthConsumerSecret(getAuthConsumerSecret())
                .setOAuthAccessToken(getAuthAccessToken())
                .setOAuthAccessTokenSecret(getAuthAccessTokenSecret());
        cb.setJSONStoreEnabled(true);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(new TwitterStatusListener(producer));
        twitterStream.sample();
//        twitterStream.firehose(10000);
        return;

    }

    @Bean
    public ProducerConfig getProducerConfig() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
//        props.put("metadata.broker.list", "broker1:9092,broker2:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        producerConfig = new ProducerConfig(props);
        return producerConfig;

    }

    @Bean
    public Producer<String, String> getProducer() {
        producer = new Producer<String, String>(producerConfig);
        return producer;
    }

    public String getAuthConsumerKey() {
        return authConsumerKey;
    }

    public void setAuthConsumerKey(String authConsumerKey) {
        this.authConsumerKey = authConsumerKey;
    }

    public String getAuthConsumerSecret() {
        return authConsumerSecret;
    }

    public void setAuthConsumerSecret(String authConsumerSecret) {
        this.authConsumerSecret = authConsumerSecret;
    }

    public String getAuthAccessToken() {
        return authAccessToken;
    }

    public void setAuthAccessToken(String authAccessToken) {
        this.authAccessToken = authAccessToken;
    }

    public String getAuthAccessTokenSecret() {
        return authAccessTokenSecret;
    }

    public void setAuthAccessTokenSecret(String authAccessTokenSecret) {
        this.authAccessTokenSecret = authAccessTokenSecret;
    }
}
