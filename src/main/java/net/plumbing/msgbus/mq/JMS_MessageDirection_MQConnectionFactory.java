package net.plumbing.msgbus.mq;

// import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import org.slf4j.Logger;
import javax.jms.JMSException;
import java.net.InetAddress;

// JMS_MessageDirection_MQConnectionFactory вынесен в поток jmsReceiveTask
public class JMS_MessageDirection_MQConnectionFactory {
    // public static PooledConnectionFactory MQconnectionFactory=null;
    /*
    public static String fBrokerURL;
    public static String fUserName; 
    public static String fPassword;
    public static String fClientID;

    public static  ActiveMQConnectionFactory
                  MakeActiveMQConnectionFactory(String brokerURL, String pUserName, String pPassword, Logger JMSReceiveTask_Log ) throws JMSException {

        JMSReceiveTask_Log.info("ActiveMQConnectionFactory MsgBus preSet");
        JMSReceiveTask_Log.info("ActiveMQConnectionFactory MsgBus TCP connect: [" + brokerURL + "]");
        String localHostAddress;
        try {
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch ( java.net.UnknownHostException e )
        {JMSReceiveTask_Log.warn( "InetAddress.getLocalHost().getHostAddress(): " + e.getMessage());
            localHostAddress = "xxx";
        }

        //fBrokerURL = brokerURL;
        //fUserName = pUserName;
        //fPassword = pPassword;
        //fClientID = "JMS.Receiver.Q." + localHostAddress;


        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setClientID("JMS.Receiver.Q." + localHostAddress);
        connectionFactory.setClientIDPrefix("ReceiverQ");
        connectionFactory.setMaxThreadPoolSize(50);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setAlwaysSessionAsync(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setCopyMessageOnSend(false);
        if ( pUserName != null) connectionFactory.setUserName(pUserName); else connectionFactory.setUserName("");
        if ( pPassword != null) connectionFactory.setPassword(pPassword); else connectionFactory.setPassword("");


        // Create a Connection
        PooledConnectionFactory PooledConnectionF = new PooledConnectionFactory(connectionFactory);
       // JMS_MessageDirection_MQConnectionFactory.MQconnectionFactory = PooledConnectionF;


        return connectionFactory;
    }
*/
}
