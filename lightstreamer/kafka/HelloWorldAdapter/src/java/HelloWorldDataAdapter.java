/*
 * Copyright 2013 Weswit Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.*;
import java.io.File;
import com.lightstreamer.interfaces.data.*;

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

public class HelloWorldDataAdapter implements SmartDataProvider {

    private ItemEventListener listener;
    private volatile GreetingsThread gt;

    public void init(Map params, File configDir) throws DataProviderException {
    }

    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
    }

    public void setListener(ItemEventListener listener) {
        this.listener = listener;
    }

    public void subscribe(String itemName, Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        if (itemName.equals("greetings")) {
            gt = new GreetingsThread(itemHandle);
            gt.start();
        }
    }
    
    public void subscribe(String itemName, boolean needsIterator)
                throws SubscriptionException, FailureException {
    }         	

    public void unsubscribe(String itemName) throws SubscriptionException,
            FailureException {
        if (itemName.equals("greetings") && gt != null) {
            gt.go = false;
        }
    }

    class GreetingsThread extends Thread {

        private final Object itemHandle;
        public volatile boolean go = true;

        public GreetingsThread(Object itemHandle) {
            this.itemHandle = itemHandle;
        }

        public void run() {
          try {
            Properties props = new Properties();
             props.setProperty("metadata.broker.list","localhosT:9092");
             ConsumerConfiguration conf = new ConsumerConfiguration(props);
             Consumer consumer = new Consumer(conf, "clientId", "test", 0);
             //Consumer consumer = new Consumer(conf, "clientId", "test");

             // Use a byte buffer to store the message you retrieve.
             byte[] buffer = new byte[1024];
             int bytesRead;
             while ( go ) {
                bytesRead = consumer.getMessage(buffer, 0, buffer.length);
                if (bytesRead > 0) {
                  Map<String, String> data = new HashMap<String, String>();
                  // the first bytesRead bytes of the buffer are the message.
                  //System.out.println(new String(buffer,0, bytesRead));

                  //data.put("message", new String(buffer,0, bytesRead));
                  data.put("message",   new String(buffer,0,  67));
                  data.put("message_1", new String(buffer,68, 67));
                  data.put("message_2", new String(buffer,136, 67));
                  data.put("message_3", new String(buffer,204, 67));
                  data.put("message_4", new String(buffer,272, 67));
                  data.put("message_5", new String(buffer,340, 67));
                  data.put("message_6", new String(buffer,408, 67));
                  data.put("message_7", new String(buffer,476, 67));
                  data.put("message_8", new String(buffer,544, 67));
                  data.put("message_9", new String(buffer,712, 67));
                  data.put("message10", new String(buffer,780, 67));
                  data.put("message11", new String(buffer,848, 67));
                  data.put("message12", new String(buffer,916, 67));
                  data.put("timestamp", new Date().toString());
                  listener.smartUpdate(itemHandle, data, false);
                }
             }
          } catch (Exception ex) {
             ex.printStackTrace();
          }
          /*  int c = 0;
    	    Random rand = new Random();
            while(go) {
                Map<String, String> data = new HashMap<String, String>();
                data.put("message", c % 2 == 0 ? "Hellooooooo" : "Weurld");
                data.put("timestamp", new Date().toString());
                listener.smartUpdate(itemHandle, data, false);
                c++;
                try {
                    Thread.sleep(1000 + rand.nextInt(2000));
                } catch (InterruptedException e) {
                }
            }*/
        }
    }

}
