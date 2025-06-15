package net.plumbing.msgbus.mq;

import jakarta.jms.*;
//import javax.validation.constraints.NotNull;

import net.plumbing.msgbus.common.ApplicationProperties;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
// @Configuration
//

public class ActiveMQService {
    //private PooledConnectionFactory  MQpooledConnectionFactory=null;
    private ActiveMQConnectionFactory MQconnectionFactory=null;
    //private TopicConnection JMSTopicConnection=null;
    //private Connection JMSQueueConnection=null;

    private static final String DESTINATION_NAME = "Q.Sender.OUT";
    private static final String SenderDESTINATION_NAME= "Q.Sender.IN";
    private static final Logger ActiveMQService_Log = LoggerFactory.getLogger(ActiveMQService.class);


   public String  StartJMSQueueConnection( String TextMessageSring) throws JMSException, UnknownHostException, jakarta.jms.JMSException {
       Long pid = ProcessHandle.current().pid();
       jakarta.jms.Connection  Qconnection = StoreMQpooledConnectionFactory.MQpooledConnectionFactory.createConnection();
       String sClientID = "JMS.Receiver." + pid.toString() + "-" + InetAddress.getLocalHost().getHostAddress();
       Qconnection.setClientID( sClientID );
       Qconnection.start();
       Session Qsession = Qconnection.createSession(false,
               Session.AUTO_ACKNOWLEDGE);
       Destination JMSdestination = Qsession.createQueue("Q.xxx.yyyy.IN");
       MessageProducer producer = Qsession.createProducer(JMSdestination);
       // We will send a small text message saying 'Hello World!!!'
       TextMessage message = Qsession.createTextMessage("{\"testTextMessage\": \"Hello Jms!!! Welcome to the world of ActiveMQ.\" }");
       message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
       producer.send(message);
       // Qsession.commit();
       producer.close();
       Qsession.close();
       // Qsession.createTemporaryQueue()
       Qconnection.stop();
       Qconnection.close();
       //this.JMSQueueConnection = Qconnection;
       return sClientID;
   }
/*
    public  Connection SendTextMessageJMSQueue( String TextMessageSring, String QueueName ) throws JMSException {
       String localHostAddress;
        try {
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch ( java.net.UnknownHostException e )
        {ActiveMQService_Log.warn( "InetAddress.getLocalHost().getHostAddress(): " + e.getMessage());
            localHostAddress = "xxx";
        }
        Connection Qconnection = StoreMQpooledConnectionFactory.MQpooledConnectionFactory.createConnection();
        Qconnection.setClientID("JMS.Receiver."  + localHostAddress );
        Qconnection.start();
        Session Qsession = Qconnection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Destination JMSdestination = Qsession.createQueue(QueueName);
        MessageProducer producer = Qsession.createProducer(JMSdestination);
        // We will send a small text message saying 'Hello World!!!'
        TextMessage message = Qsession.createTextMessage( TextMessageSring );
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(message);
        // Qsession.commit();
        producer.close();
        Qsession.close();
        // Qsession.createTemporaryQueue()
        Qconnection.stop();
        Qconnection.close();
        //this.JMSQueueConnection = Qconnection;
        return Qconnection;
    }
    */
    // @Bean
    public ActiveMQConnectionFactory MakeActiveMQConnectionFactory(String brokerURL ) throws Exception {
        String localHostAddress;
        Long pid = ProcessHandle.current().pid();
        try {
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch ( java.net.UnknownHostException e )
        {ActiveMQService_Log.warn( "InetAddress.getLocalHost().getHostAddress(): " + e.getMessage());
            localHostAddress = "xxx";
        }
        String sClientID= "JMS.Interlal." + pid.toString() + "-" + localHostAddress;
        ActiveMQService_Log.info("ActiveMQConnectionFactory MsgBus preSet");
        ActiveMQService_Log.info("ActiveMQConnectionFactory MsgBus TCP connect: [" + ApplicationProperties.ConnectMsgBus + "]");

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setClientID( sClientID );
        connectionFactory.setClientIDPrefix("ReceiverI");
        connectionFactory.setMaxThreadPoolSize(50);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setAlwaysSessionAsync(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setCopyMessageOnSend(false);
        // Create a Connection
        this.MQconnectionFactory = connectionFactory;
        PooledConnectionFactory PooledConnectionF = new PooledConnectionFactory(connectionFactory);
        StoreMQpooledConnectionFactory.MQpooledConnectionFactory = PooledConnectionF;

        return connectionFactory;
    }


}
