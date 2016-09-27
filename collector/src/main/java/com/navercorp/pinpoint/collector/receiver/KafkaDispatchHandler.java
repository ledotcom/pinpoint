package com.navercorp.pinpoint.collector.receiver;

import com.navercorp.pinpoint.collector.handler.AgentInfoHandler;
import com.navercorp.pinpoint.collector.handler.Handler;
import com.navercorp.pinpoint.collector.handler.RequestResponseHandler;
import com.navercorp.pinpoint.collector.handler.SimpleHandler;
import com.navercorp.pinpoint.thrift.dto.*;

import org.apache.thrift.TBase;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
/**
 * 
 * @Description:
 * @Author denghong1
 * @Create time: 2016年8月3日下午6:46:53
 *
 */
public class KafkaDispatchHandler extends AbstractDispatchHandler {

    @Autowired()
    @Qualifier("agentInfoHandler")
    private AgentInfoHandler agentInfoHandler;

    @Autowired()
    @Qualifier("sqlMetaDataHandler")
    private RequestResponseHandler sqlMetaDataHandler;

    @Autowired()
    @Qualifier("apiMetaDataHandler")
    private RequestResponseHandler apiMetaDataHandler;

    @Autowired()
    @Qualifier("stringMetaDataHandler")
    private RequestResponseHandler stringMetaDataHandler;
    
    //udpstat
    @Autowired()
    @Qualifier("agentStatHandler")
    private Handler agentStatHandler;
    
    // udpspan
    @Autowired()
    @Qualifier("spanHandler")
    private SimpleHandler spanDataHandler;

    @Autowired()
    @Qualifier("spanChunkHandler")
    private SimpleHandler spanChunkHandler;



    public KafkaDispatchHandler() {
        this.logger = LoggerFactory.getLogger(this.getClass());
    }
    
    /**
     * 重写此方法，kafka的handle只会调用此方法，不会像tcpReceiver那样去判断是否需要respone，因此对于apiMetaData那样的数据没有处理，因此需要重新
     */
    @Override
    public void dispatchSendMessage(TBase<?, ?> tBase) {
        // mark accepted time
        acceptedTimeService.accept();
        
        // TODO consider to change dispatch table automatically
        SimpleHandler simpleHandler = getSimpleHandler(tBase);
        if (simpleHandler != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("simpleHandler name:{}", simpleHandler.getClass().getName());
            }
            simpleHandler.handleSimple(tBase);
            return;
        }

        Handler handler = getHandler(tBase);
        if (handler != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("handler name:{}", handler.getClass().getName());
            }
            handler.handle(tBase);
            return;
        }
        
        RequestResponseHandler requestResponseHandler = getRequestResponseHandler(tBase);
        if (requestResponseHandler != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("requestResponseHandler name:{}", requestResponseHandler.getClass().getName());
            }
            requestResponseHandler.handleRequest(tBase);
            return; //kafka handle不返回handleRequest方法的TBase result
        }

        throw new UnsupportedOperationException("Handler not found. Unknown type of data received. tBase=" + tBase);
    }


    @Override
    RequestResponseHandler getRequestResponseHandler(TBase<?, ?> tBase) {
        if (tBase instanceof TSqlMetaData) {
            return sqlMetaDataHandler;
        }
        if (tBase instanceof TApiMetaData) {
            return apiMetaDataHandler;
        }
        if (tBase instanceof TStringMetaData) {
            return stringMetaDataHandler;
        }
        if (tBase instanceof TAgentInfo) {
            return agentInfoHandler;
        }
        return null;
    }

    @Override
    SimpleHandler getSimpleHandler(TBase<?, ?> tBase) {
        if (tBase instanceof TAgentInfo) {
            return agentInfoHandler;
        }
        if (tBase instanceof TSpan) {
            return spanDataHandler;
        }
        if (tBase instanceof TSpanChunk) {
            return spanChunkHandler;
        }
        return null;
    }
    
    @Override
    Handler getHandler(TBase<?, ?> tBase) {

        // To change below code to switch table make it a little bit faster.
        // FIXME (2014.08) Legacy - TAgentStats should not be sent over the wire.
        if (tBase instanceof TAgentStat || tBase instanceof TAgentStatBatch) {
            return agentStatHandler;
        }
        return null;
    }
}
