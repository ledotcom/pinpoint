package com.navercorp.pinpoint.collector.receiver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.pinpoint.bootstrap.util.StringUtils;
import com.navercorp.pinpoint.common.util.ExecutorFactory;
import com.navercorp.pinpoint.common.util.PinpointThreadFactory;
import com.navercorp.pinpoint.rpc.util.CpuUtils;
import com.navercorp.pinpoint.thrift.io.DeserializerFactory;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializerFactory;
import com.navercorp.pinpoint.thrift.io.ThreadLocalHeaderTBaseDeserializerFactory;
import com.navercorp.pinpoint.thrift.util.SerializationUtils;
/**
 * 
 * @Description:
 * @Author denghong1
 * @Create time: 2016年8月3日下午7:37:11
 *
 */
public class KafkaReceiver {

    private final Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);

    private final ThreadFactory kafkaWorkerThreadFactory = new PinpointThreadFactory("Pinpoint-Kafka-Worker");
    private final DispatchHandler dispatchHandler;
    

    private final ThreadPoolExecutor worker;

    private final DeserializerFactory<HeaderTBaseDeserializer> deserializerFactory = new ThreadLocalHeaderTBaseDeserializerFactory<>(new HeaderTBaseDeserializerFactory());
    
	private Properties consumerProperties;

	private String topic;

	private int kafkaWorkerThreadNum;
	
	private static ConsumerConnector consumer;
	
	private final AtomicBoolean state = new AtomicBoolean(true);
    
    public KafkaReceiver(Properties consumerProperties, DispatchHandler dispatchHandler) {
    	if (consumerProperties == null) {
    		throw new NullPointerException("kafkaConsumer properties file must not be null");
    	}
    	// 检查基本的配置检查
    	this.topic = consumerProperties.getProperty("topic");
    	if(StringUtils.isEmpty(this.topic)){
    		throw new NullPointerException("property topic must not be null");
    	}
    	
    	// 检查基本的配置检查
    	if(StringUtils.isEmpty(consumerProperties.getProperty("zookeeper.connect"))){
    		throw new NullPointerException("property zookeeper.connect must not be null");
    	}
    	
    	if (dispatchHandler == null) {
    		throw new NullPointerException("dispatchHandler must not be null");
    	}
    	this.consumerProperties = consumerProperties;
    	this.dispatchHandler = dispatchHandler;
    	this.kafkaWorkerThreadNum = Integer.parseInt(consumerProperties.getProperty("collector.kafkaWorkerThread", ""+CpuUtils.workerCount()));
    	this.worker = ExecutorFactory.newFixedThreadPool(kafkaWorkerThreadNum, Integer.parseInt(consumerProperties.getProperty("collector.kafkaWorkerQueueSize", ""+1024 * 5)), kafkaWorkerThreadFactory);
    }
    

    @PostConstruct
    public void start() {
        // init kafkaconsumer
        this.consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        ConsumerConfig config = new ConsumerConfig(this.consumerProperties);

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(this.topic, this.kafkaWorkerThreadNum);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			this.worker.execute(new ConsumerMsgTask(stream));
		}

    }
    
    private class ConsumerMsgTask implements Runnable {
    	private KafkaStream<byte[], byte[]> m_stream;

    	public ConsumerMsgTask(KafkaStream<byte[], byte[]> stream) {
    		m_stream = stream;
    	}

		public void run() {
			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			byte[] databytes;
			while (state.get() && it.hasNext()) {
				try {
					databytes = it.next().message();
					TBase<?, ?> tBase = SerializationUtils.deserialize(databytes, deserializerFactory);
					//logger.info("$$$$$$$$$$receive:" + tBase);
					dispatchHandler.dispatchSendMessage(tBase);
				} catch (TException e) {
					logger.error("packet serialize error. Cause:{}", e.getMessage());
				} catch (Exception e) {
					logger.error("consumer message error.Cause:{}", e.getMessage());
				}
			}
		}
    }

    @PreDestroy
    public void stop() {
        logger.info("Pinpoint-Kafka-Server stop");
        state.set(false);
        if(null != consumer){
        	consumer.shutdown();
        }
        shutdownExecutor(worker,"kafkaConsumer");
    }
    
    private void shutdownExecutor(ExecutorService executor,String executorName) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        	logger.info("{}.shutdown() Interrupted", executorName, e);
            Thread.currentThread().interrupt();
        }
    }
}
