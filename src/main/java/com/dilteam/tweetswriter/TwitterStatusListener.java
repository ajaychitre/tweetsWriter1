package com.dilteam.tweetswriter;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import twitter4j.*;

import java.util.Date;

@ComponentScan
@EnableAutoConfiguration
@ConfigurationProperties
public class TwitterStatusListener implements StatusListener {
    @Autowired
    private Producer<String, String> producer;

    private static long counter = 0;

    public TwitterStatusListener(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void onStatus(Status status) {
        String json = TwitterObjectFactory.getRawJSON(status);
        if (++counter % 10000 == 0) {
            System.out.println("$$$$$$  Total no. of tweets so far: " + counter + " at: "
                    + new Date(System.currentTimeMillis()).toString());
        }
        if (producer != null) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("tweets", json);
            producer.send(data);
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//        System.out.println("onDeletionNotice: " + statusDeletionNotice.toString());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("onTrackLimitationNotice: " + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("onScrubGeo: " + userId + "\t" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        System.out.println("onStallWarning: " + warning.toString());
    }

    @Override
    public void onException(Exception ex) {
        System.out.println("onException: " + ex.getMessage());
    }
}
