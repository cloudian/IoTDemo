// Kodiak Conrad
// June 24, 2018
// Kafka Demo Remote producer
package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.lang.Runnable;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class DemoProducer {

		private final static String TOPIC = "new-topic";
		private final static String BOOTSTRAP_SERVERS = "10.10.0.86:9092";
		private final static String intSerializer = "org.apache.kafka.common.serialization.IntegerSerializer";
		private final static String stringSerializer ="org.apache.kafka.common.serialization.StringSerializer";
		private final static int SECONDS = 1;
		private final static int PARTITIONS = 0; //zero indexed

		public static void main(String[] args) throws Exception {
			//runProducer(5);
			ScheduledExecutorService readData = Executors.newScheduledThreadPool(5);
			Runnable runnable = new Runnable() {
				public void run() {
					try {
						runProducer();
					} catch (Exception e) {
						Thread t = Thread.currentThread();
    					t.getUncaughtExceptionHandler().uncaughtException(t, e);
					}
				}
			};
			readData.scheduleAtFixedRate(runnable, 0, 1000*SECONDS, TimeUnit.MILLISECONDS);
		}

		private static Producer<Integer, String> createProducer() {
			Properties props = new Properties();
			props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
			props.put("key.serializer", intSerializer);
			props.put("value.serializer", stringSerializer);
			KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
			return producer;
		}

		public static void runProducer() throws Exception {
			final Producer<Integer, String> producer = createProducer();
			int partition = 0;
			try {
				//String data = "fake shit";
				String data = getTemp();
				final ProducerRecord<Integer, String> record = new ProducerRecord<>(
					TOPIC, /*partition, timestamp,*/partition, data);

				RecordMetadata metadata = producer.send(record/*, new DemoProducerCallback()*/).get();
				partition = increment(partition, PARTITIONS);

				  /*
				  long elapsedTime = System.currentTimeMillis() - time;
				  System.out.printf("sent record(key=%s value=%s) " +
								  "meta(partition=%d, offset=%d) time=%d\n",
						  record.key(), record.value(), metadata.partition(),
						  metadata.offset(), elapsedTime);
						  */

				
				//input.close();

		  	} finally {
			  producer.flush();
			  producer.close();
		  	}
		}

		private static int increment(int k, int n) {
			if (k < n) {
				k++;
			} else {
				k = 0;
			}
			return k;
		}

		public static String getTemp() throws Exception {
			Runtime rt = Runtime.getRuntime();
	                Process p = rt.exec("python /home/pi/DHT11_Python/trial.py");
			//Process p = rt.exec("echo /home/pi/DHT11_Python/trial.py");
			BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			if ((line = bri.readLine()) != null) {
				System.out.println(line);
			} else {
				System.out.println("This failed");
			}
			bri.close();
			return line;
		}

		

		/*
		private class DemoProducerCallback implements Callback {
				@Override
			public static void onCompletion(RecordMetadata RecordMetadata, Exception e) {
				if (e != null) {
					System.out.println(e);
				}
			}

		}
		*/
}






