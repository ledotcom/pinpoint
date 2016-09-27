/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.profiler.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.pinpoint.bootstrap.util.StringUtils;
import com.navercorp.pinpoint.profiler.sender.AbstractDataSender.RequestMarker;
import com.navercorp.pinpoint.rpc.FutureListener;
import com.navercorp.pinpoint.rpc.ResponseMessage;
import com.navercorp.pinpoint.rpc.client.PinpointClientReconnectEventListener;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializerFactory;
/**
 * 
 * @Description:span数据进入到kafka中
 * @Author denghong1
 * @Create time: 2016年7月20日下午5:01:36
 *
 */
public class KafkaDataSender extends AbstractDataSender implements EnhancedDataSender  {
	public static final String DEFAULT_TIMEOUT_MILLIS = "30000";
	
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final boolean isDebug = logger.isDebugEnabled();
    
    private static final String PROPERTYSPLITTER_STRING = "\\|\\|";
	private static final String KEYVALUESPLITTER_STRING = "\\=";

	private String topic;
    private List<Producer<byte[], byte[]>> producerList;
    
    private int producerIndex = 0;
    private int timeoutMillis;
    
    // Caution. not thread safe
    //private final HeaderTBaseSerializer serializer = HeaderTBaseSerializerFactory.DEFAULT_FACTORY.createSerializer();
    // 由于serializer是非线程安全的，因此为每个线程单独设置一个serializer
    private ThreadLocal<HeaderTBaseSerializer> serializerThreadLocal = new ThreadLocal<HeaderTBaseSerializer>(){
    	 @Override
         protected HeaderTBaseSerializer initialValue() {
             return HeaderTBaseSerializerFactory.DEFAULT_FACTORY.createSerializer();
         }
    };

    private final AsyncQueueingExecutor<Object> executor;


    public KafkaDataSender(String topic, String servers, int producerSize, String properties,String threadName, int queueSize) {
        if (topic == null ) {
            throw new NullPointerException("topic must not be null");
        }
        if (servers == null ) {
            throw new NullPointerException("servers must not be null");
        }
        if (producerSize <= 0) {
        	throw new IllegalArgumentException("producerSize");
        }
        if (threadName == null) {
            throw new NullPointerException("threadName must not be null");
        }
        if (queueSize <= 0) {
        	throw new IllegalArgumentException("queueSize");
        }

        // TODO If fail to create socket, stop agent start
        logger.info("KafkaDataSender initialized. topic={}, servers={}, producerSize={}, properties={}", topic, servers,producerSize,properties);
        this.topic = topic;
        createProducerList(servers,producerSize,properties);

        this.executor = createAsyncQueueingExecutor(queueSize, threadName);
    }

    private void createProducerList(String servers, int producerSize, String properties) {
    	Properties config = new Properties();
		config.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    config.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
	    config.setProperty("batch.size", "0");
	    if(!StringUtils.isEmpty(properties)){
	    	resolveProperties(properties,config);
	    }
	    // 以参数设置的为准
	    config.setProperty("bootstrap.servers", servers);
	    config.setProperty("acks",config.getProperty("acks", "0")); 
	    
	    this.timeoutMillis = Integer.parseInt(config.getProperty("timeout.ms", DEFAULT_TIMEOUT_MILLIS));
	    int size = (producerSize < 1)?1:producerSize; //确配置的优先级高于properties配置的
	    this.producerList = new ArrayList<Producer<byte[], byte[]>>(size);
	    for(int i=0;i<size;i++){
    		this.producerList.add(new KafkaProducer<byte[], byte[]>(config));
    	}
	}
    
    private void resolveProperties(String proStr, Properties config) {
		String[] keyvaluePairs = proStr.split(PROPERTYSPLITTER_STRING);
		for (String keyvalueStr : keyvaluePairs) {
			String[] keyvaluePair = keyvalueStr.split(KEYVALUESPLITTER_STRING);
			if (keyvaluePair.length == 2) {
				config.setProperty(keyvaluePair[0], keyvaluePair[1]);
			}
		}
	}
    
    @Override
    public boolean isNetworkAvailable() {
        if (this.producerList == null) {
            return false;
        }
        return true;
    }

	@Override
    public boolean send(TBase<?, ?> data) {
        return executor.execute(data);
    }

    @Override
    public void stop() {
        executor.stop();
    }

	protected void sendPacket(Object message) {
		logger.debug("hand kafadatasender sendPacket:" + message);
		TBase tBase;
		if (message instanceof TBase) {
			tBase = (TBase) message;
		} else if (message instanceof RequestMarker) {
			RequestMarker requestMarker = (RequestMarker) message;
			tBase = requestMarker.getTBase();
		} else {
			logger.warn("sendPacket fail. invalid type:{}", message != null ? message.getClass() : null);
			return;
		}

		// do not copy bytes because it's single threaded
		final byte[] internalBufferData = serialize(serializerThreadLocal.get(), tBase);
		if (internalBufferData == null) {
			logger.warn("interBufferData is null");
			return;
		}

		final int internalBufferSize = this.serializerThreadLocal.get().getInterBufferSize();

		try {
			getNextProducer().send(new ProducerRecord<byte[], byte[]>(this.topic, internalBufferData)).get(timeoutMillis, TimeUnit.MILLISECONDS);
			if (isDebug) {
				logger.debug("Data sent. size:{}, {}", internalBufferSize, tBase);
			}
		} catch (Exception e) {
			logger.error("packet send error. size:{}, {}", internalBufferSize, tBase, e);
		}
	}
    
    private Producer<byte[], byte[]> getNextProducer() {
    	int index = producerIndex++;
    	if(index >= producerList.size()){
    		index = producerIndex = 0;
    	}
		return producerList.get(index);
	}

	@Override
	public boolean request(TBase<?, ?> data) {
		return this.request(data, 1); // kafka只发送一次
	}

	@Override
	public boolean request(TBase<?, ?> data, int retryCount) {
		RequestMarker message = new RequestMarker(data, retryCount);
        return executor.execute(message);
	}

	@Override
	public boolean request(TBase<?, ?> data, FutureListener<ResponseMessage> listener) {
		 RequestMarker message = new RequestMarker(data, listener);
	     return executor.execute(message);
	}

	@Override
	public boolean addReconnectEventListener(PinpointClientReconnectEventListener eventListener) {
		return false;
	}

	@Override
	public boolean removeReconnectEventListener(PinpointClientReconnectEventListener eventListener) {
		return false;
	}
}
