package com.clickstream;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

@Slf4j
public class Main {
    static int userNum = 15;
    static int durationSecond = 300;
    static Set<String> ipSet = new HashSet<>();
    static Random random = new Random();

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(userNum);
        ExecutorService executor = Executors.newFixedThreadPool(userNum);
        IntStream.range(0, userNum).forEach(i -> {
            String ipAddr = getIpAddr();
            executor.execute(new LogGenerator(latch, ipAddr,
                    UUID.randomUUID().toString(), durationSecond));
        });
        executor.shutdown();

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error(String.valueOf(e));
        }
    }

    private static String getIpAddr() {
        while (true) {
            String ipAddr = "192.168.0." + random.nextInt(256);
            if (!ipSet.contains(ipAddr)) {
                ipSet.add(ipAddr);
                return ipAddr;
            }
        }
    }
}
