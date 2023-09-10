
/*
 * @(#)Consumidor.java
 *
 * Copyright (c) BANCO DE CHILE (Chile). All rights reserved.
 *
 * All rights to this product are owned by BANCO DE CHILE and may only
 * be used under the terms of its associated license document. You may NOT
 * copy, modify, sublicense, or distribute this source file or portions of
 * it unless previously authorized in writing by BANCO DE CHILE.
 * In any event, this notice and the above copyright must always be included
 * verbatim with this file.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consumidor.
 *
 * @author Claudio Quinteros.
 * @version 1.0.0, 10-09-2023
 */
public class Consumidor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "grupo1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("fetch.min.bytes", "1");
        props.put("fetch.max.wait.ms", "500");
        props.put("max.partition.fetch.bytes", "1048576");
        props.put("session.timeout.ms", "10000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,
                String>(props);
        try{
            consumer.subscribe(Collections.singletonList("topic-test"));
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> record : records){
                    System.out.print("Topic: " + record.topic() + ", ");
                    System.out.print("Partition: " + record.partition() + ", ");
                    System.out.print("Key:" + record.key() + ", ");
                    System.out.println("Value: " + record.value() + ", ");
                }
            }
        } catch (Exception e){e.printStackTrace();}
        finally {
            consumer.close();}
    }
}