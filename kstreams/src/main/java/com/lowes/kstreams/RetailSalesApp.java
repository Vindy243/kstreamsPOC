package com.lowes.kstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lowes.kstreams.model.Product;
import com.lowes.kstreams.model.Purchase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class RetailSalesApp {

	public void startStreamProcessing() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "retail-sales-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		Serde<Purchase> purchaseSerde = getJsonSerde(Purchase.class);
		Serde<Product> productSerde = getJsonSerde(Product.class);
		Serde<Double> doubleSerde = Serdes.Double();

		KStream<String, Purchase> purchases = builder.stream("purchases", Consumed.with(Serdes.String(), purchaseSerde));
		KTable<String, Product> products = builder.table("products", Consumed.with(Serdes.String(), productSerde));

		KStream<String, Double> salesStream = purchases
				.join(products,
						(purchase, product) -> purchase.getQuantity() * product.getPrice(),
						Joined.with(Serdes.String(), purchaseSerde, productSerde)
				);

		TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2));

		KTable<Windowed<String>, Double> salesAgg = salesStream
				.groupByKey(Grouped.with(Serdes.String(), doubleSerde))
				.windowedBy(timeWindows)
				.reduce(Double::sum, Materialized.as("sales-per-product"));

		salesAgg.toStream()
				.map((windowedKey, total) -> {
					String productId = windowedKey.key();
					String formattedValue = "Product: " + productId + ", Total Sales: " + total;
					return KeyValue.pair(productId, formattedValue);
				})
				.to("total-sales", Produced.with(Serdes.String(), Serdes.String()));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		Runtime.getRuntime().addShutdownHook((new Thread(streams::close)));
	}

	private static <T> Serde<T> getJsonSerde(Class<T> clazz) {
		ObjectMapper mapper = new ObjectMapper();
		Serializer<T> serializer = (topic, data) -> {
			try {
				return mapper.writeValueAsBytes(data);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		};
		Deserializer<T> deserializer = (topic, data) -> {
			try {
				return mapper.readValue(data, clazz);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		};
		return Serdes.serdeFrom(serializer, deserializer);
	}
}
