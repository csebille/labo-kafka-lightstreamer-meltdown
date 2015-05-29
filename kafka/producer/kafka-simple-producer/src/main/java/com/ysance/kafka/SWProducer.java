package com.ysance.kafka;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
 
public class SWProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);

        Path file  = Paths.get("/opt/starwars/sw1.txt");

        int frameLinesNumber = 14;

        Charset charset = Charset.forName("UTF8");
        StringBuffer sb = new StringBuffer();
        try (BufferedReader reader = Files.newBufferedReader(file,charset)) {
            String line = null;
            int lineIndex = 0;
            int displayTime = 1;
            while ((line = reader.readLine()) != null) {
                lineIndex++;
                if (lineIndex == 1) {
                    // If we are on the first line and StringBuffer is not empty, play the frame a displayTime times
                    if (sb.length() > 0) {
                        float duration = displayTime * 1000 / 15;
                        BigDecimal bdDuration = new BigDecimal(duration);
                        try {
                            Thread.sleep(bdDuration.intValue());
                        } catch (Exception ex) {

                        }
                        for (int frameIndex = 0; frameIndex < displayTime - 1; frameIndex++  ) {
                            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", sb.toString());
                            producer.send(data);
                            System.out.println();
                        }
                    }
                    // Reseting diplayTime with the value found in the first line and the buffer
                    sb = new StringBuffer();
                    displayTime = 1;
                    try {
                        displayTime = Integer.parseInt(line.trim());
                    }  catch (Exception ex) {

                    }
                } else {
                    sb.append(line).append("\n");
                    if (lineIndex % frameLinesNumber == 0) {
                        lineIndex = 0;
                    }
                }
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        producer.close();
    }
}