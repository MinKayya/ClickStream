package com.clickstream;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class LogGenerator implements Runnable{

    private CountDownLatch latch;
    private String ipAddr;
    private String sessionId;
    private int durationSecond;
    private Random random;
    private final static long  MINIMUM_SLEEP_TIME = 500;
    private final static long  MAXIMUM_SLEEP_TIME = 60000;
    private  final  static String TOPIC_NAME = "webLog";

    public LogGenerator(CountDownLatch latch, String ipAddr, String sessionId, int durationSecond) {
        this.latch = latch;
        this.ipAddr = ipAddr;
        this.sessionId = sessionId;
        this.durationSecond = durationSecond;
        this.random = new Random();
    }

    @Override
    public void run() {
        log.info("Starting log generator (ipAddr = " + ipAddr + ", sessionId = " + sessionId +
             ", durationSecond = " + durationSecond);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "LogGenerator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        long startTime = System.currentTimeMillis();

        while (isDuration(startTime)){
            long sleepTime = MINIMUM_SLEEP_TIME + Double.valueOf(random.nextDouble() *
                    (MAXIMUM_SLEEP_TIME - MINIMUM_SLEEP_TIME)).longValue();
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e){
                e.printStackTrace();
            }

            String responseCode = getResponseCode();
            String responseTime = getResponseTime();
            String method = getMethod();
            String url = getUrl();
            OffsetDateTime offsetDateTime= OffsetDateTime.now(ZoneId.of("UTC"));

            String logData = String.format("%s %s %s %s %s %s %s",
                    ipAddr, offsetDateTime, method, url, responseCode, responseTime, sessionId);
            log.info(logData);

            producer.send(new ProducerRecord<>(TOPIC_NAME, logData));
        }
        producer.close();

        log.info("Stopping LogGenerator (ipAddr = " + ipAddr +  ", sessionId = " + sessionId +
                ", durationSecond = " + durationSecond);

        this.latch.countDown();
    }

    private boolean isDuration(long startTime) {
        return System.currentTimeMillis() - startTime < durationSecond * 1000L;
    }

    private String getResponseCode() {
        String responseCode = "200";
        if(random.nextDouble() > 0.97) {
            responseCode = "404";
        }
        return responseCode;
    }

    private String getResponseTime() {
        int responseTime = 1000 + random.nextInt(901);
        return String.valueOf(responseTime);
    }
    private String getMethod() {
        if (random.nextDouble() > 0.7) {
            return "POST";
        } else {
            return "GET";
        }
    }

    private String getUrl() {
        double randomValue = random.nextDouble();
        if(randomValue > 0.9) {
            return "/sub/page";
        } else if (randomValue > 0.8) {
            return "/doc/page";
        } else{
            return "/main/page";
        }
    }
}