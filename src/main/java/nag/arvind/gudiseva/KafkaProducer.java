package nag.arvind.gudiseva;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaProducer {

    public void producerTest() throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

        String topic = "magbqxyq-gudiseva";
        String brokers = "moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094";
        String username = "magbqxyq";
        String password = "bsbNWMgu5o0GUe8nWVO2JAasyxJFW2VJ";
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);

        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", username + "-consumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);

        org.apache.kafka.clients.producer.KafkaProducer<String,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String,String>(props);

        for (int i=0; i<100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,"id_"+Integer.toString(i),"Hello World" + Integer.toString(i));
            RecordMetadata metadata = producer.send(record).get();
            System.out.println( "topic:" + metadata.topic() + "partition:" + metadata.partition() + "offset:" + metadata.offset() + "timestamp:" + metadata.timestamp());
        }
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer kafkaProducer = new KafkaProducer();
        kafkaProducer.producerTest();
    }
}