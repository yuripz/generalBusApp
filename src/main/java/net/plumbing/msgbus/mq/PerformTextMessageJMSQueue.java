package net.plumbing.msgbus.mq;

import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;

import javax.jms.*;

public class PerformTextMessageJMSQueue {
    private Connection Qconnection;
    private MessageConsumer ReplyConsumer;
    private Session Qsession;

    public PerformTextMessageJMSQueue() {
        this.Qconnection =null;
        this.Qsession = null;
        this.ReplyConsumer = null;
    }

    public Connection SendTextMessageJMSQueue(String TextMessageSring, String QueueName, PooledConnectionFactory MQpooledConnectionFactory ) throws JMSException {

        this.Qconnection = MQpooledConnectionFactory.createConnection();
        //this.Qconnection.setClientID("JMS.Receiver." + Thread.currentThread().getName() ); ! // fault:Setting clientID on a used Connection is not allowed
        this.Qconnection.start();

        this.Qsession = Qconnection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Destination JMSdestination = this.Qsession.createQueue(QueueName);
        MessageProducer producer = this.Qsession.createProducer(JMSdestination);
        // We will send a small text message saying 'Hello World!!!'
        TextMessage message = this.Qsession.createTextMessage( TextMessageSring );
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        ////////////////////////////////////
        Destination tempDestResponce = this.Qsession.createTemporaryQueue();
        message.setJMSReplyTo( tempDestResponce );

        this.ReplyConsumer = this.Qsession.createConsumer(tempDestResponce);
        producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
        producer.send(message);
        producer.close();

        return this.Qconnection;
    }

    public String ReadTextMessageReplyQueue ( int TimeOut, boolean IsDebugged, Logger MessegeReceive_Log) throws JMSException {
        // JMSconsumer надо читать с блокировкой
        if ( this.ReplyConsumer != null) {

            TextMessage JMSTextMessage = (TextMessage) this.ReplyConsumer.receive(TimeOut * 1);
            if (JMSTextMessage != null) {
                String JMSMessageID = JMSTextMessage.getJMSMessageID();
                if (IsDebugged)
                    MessegeReceive_Log.info("Received message: (" + JMSMessageID + ") [" + JMSTextMessage.getText() + "]");
                return JMSTextMessage.getText();
            }
        }
        return null;
    }
    public void Stop_and_Close_MessageJMSQueue ( Long Queue_Id,  Logger MessegeReceive_Log)  {
        try {
        if ( this.ReplyConsumer != null)
            this.ReplyConsumer.close();
        /////////////////////////////////
        if ( this.Qsession != null )
            this.Qsession.close();
            if ( this.Qconnection != null ) {
                this.Qconnection.stop();
                this.Qconnection.close();
            }
        } catch ( JMSException e) { // | InterruptedException
            MessegeReceive_Log.error("Message Stop_and_Close_MessageJMSQueue()  Task[" + Queue_Id + "]: fault: " + e.getMessage());
            System.err.println("Message Stop_and_Close_MessageJMSQueue()  Task[" + Queue_Id + "]: fault: ");
            e.printStackTrace();
        }
    }


    /*
    public JMSQueueContext SendTextMessageJMSQueue(String TextMessageSring, String QueueName, PooledConnectionFactory MQpooledConnectionFactory ) throws JMSException {

        Connection Qconnection =null;
        Qconnection = MQpooledConnectionFactory.createConnection();
        Qconnection.setClientID("JMS.Receiver.QUEUEz");
        Qconnection.start();
        Session Qsession = Qconnection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Destination JMSdestination = Qsession.createQueue(QueueName);
        MessageProducer producer = Qsession.createProducer(JMSdestination);
        // We will send a small text message saying 'Hello World!!!'
        TextMessage message = Qsession.createTextMessage( TextMessageSring );
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        ////////////////////////////////////
        Destination tempDestResponce = Qsession.createTemporaryQueue();
        message.setJMSReplyTo( tempDestResponce );
        MessageConsumer ReplyConsumer = Qsession.createConsumer(tempDestResponce);
        ////////////////////////////////////
        producer.send(message);
        // Qsession.commit();
        //////////////////////////////////
        ReplyConsumer.close();
        /////////////////////////////////
        producer.close();
        Qsession.close();
        // Qsession.createTemporaryQueue()
        Qconnection.stop();
        Qconnection.close();
        JMSQueueContext myJMSQueueContext = new JMSQueueContext();
        myJMSQueueContext.Qconnection = Qconnection;
        myJMSQueueContext.Qsession = Qsession;
        myJMSQueueContext.ReplyConsumer = ReplyConsumer;
        //this.JMSQueueConnection = Qconnection;
        //Qconnection = null;
        return myJMSQueueContext;
    }

    public   class JMSQueueContext
    {
        Connection Qconnection;
        MessageConsumer ReplyConsumer;
        Session Qsession;

    };
    */

}
