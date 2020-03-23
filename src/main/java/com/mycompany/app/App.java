package com.mycompany.app;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.mycompany.app.IKafkaConstants;
import com.mycompany.app.ConsumerCreator;
import com.mycompany.app.ProducerCreator;
import java.time.Duration;

public class App {
    public static void main(String[] args) {
      runProducer();
    //   runConsumer();
    }
    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
          ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
          if (consumerRecords.count() == 0) {
            noMessageFound++;
            if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
            // If no message found count is reached to threshold exit loop.  
                break;
            else
                continue;
          }
          //print each record. 
          consumerRecords.forEach(record -> {
              System.out.println("Record Key " + record.key());
              System.out.println("Record value " + record.value());
              System.out.println("Record partition " + record.partition());
              System.out.println("Record offset " + record.offset());
              System.out.println("\n");
           });
          // commits the offset of record to broker. 
           consumer.commitAsync();
        }
        consumer.close();
    }
    static void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            // long startTime = System.currentTimeMillis();
            // String uuid = UUID.randomUUID().toString();
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, "This is record " + index);

            try {
                    RecordMetadata metadata = producer.send(record).get();
                    // Future<RecordMetadata> metadata2 = producer.send(record, new DemoCallBack(startTime, messageNo, "This is record " + index));
                    System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
            }  
        }
    }
}


class DemoCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;
    
    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}