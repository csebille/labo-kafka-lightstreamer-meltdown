package com.ysance.kafka;

import com.blackberry.krackle.consumer.ConsumerConfiguration;
import com.blackberry.krackle.consumer.Consumer;
import com.blackberry.krackle.meta.MetaData;
import com.blackberry.krackle.producer.Producer;
import com.blackberry.krackle.producer.ProducerConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumer {

  
  public static void main(String[] args) {     
    Properties props = new Properties();
    props.setProperty("metadata.broker.list","localhost:9092");
    ProducerConfiguration conf = new ProducerConfiguration(props);
    Producer producer = new Producer(conf, "clientId", "topic", "key");
    try {    
      // Use a byte buffer to store your data, and pass the data by referencing that.
      byte[] buffer = "This is the data sent to Kafka".getBytes();

      int offset=0;      
      int length=1;
      while ( offset+length <=  buffer.length())  { // Get some data in your buffer
        producer.send(buffer, offset, length);
        offset = length + 1;
      }
    }  finally {
      producer.close();
    }
  }  
}


/*       Properties props = new Properties();
       props.setProperty("metadata.broker.list","localhost:9092");
       ConsumerConfiguration conf = new ConsumerConfiguration(props);
       Consumer consumer = new Consumer(conf, "clientId", "test", 0);
       //Consumer consumer = new Consumer(conf, "clientId", "test");

       // Use a byte buffer to store the message you retrieve.
       byte[] buffer = new byte[1024];
       int bytesRead;
       while ( true ) {
          bytesRead = consumer.getMessage(buffer, 0, buffer.length);
          if (bytesRead > 0) {
           // the first bytesRead bytes of the buffer are the message.
           System.out.println(new String(buffer,0, bytesRead));
          }
       }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  */