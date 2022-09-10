package net.plumbing.msgbus.threads;

//import org.apache.activemq.ActiveMQConnectionFactory;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.controller.PerfotmInputMessages;
import net.plumbing.msgbus.model.MessageDetails;
//import net.plumbing.msgbus.mq.JMS_MessageDirection_MQConnectionFactory;
import net.plumbing.msgbus.telegramm.NotifyByChannel;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.jdom2.input.JDOMParseException;
//import org.jdom2.input.JDOMParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import net.plumbing.msgbus.threads.utils.XMLutils;


import javax.jms.*;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static net.plumbing.msgbus.common.XMLchars.*;
import static net.plumbing.msgbus.common.XMLchars.Body_noNS_Begin;

public class JMSReceiveTask  implements Runnable {
    private Connection Qconnection;
    private MessageConsumer JMS_Q_Consumer;
    private Session Qsession;
    private xlstErrorListener XSLTErrorListener;
    private TheadDataAccess theadDataAccess;
    private ActiveMQConnectionFactory runMQConnectionFactory;
    
    public JMSReceiveTask() {
        super();
        this.Qconnection =null;
        this.Qsession = null;
        this.JMS_Q_Consumer = null;
        this.XSLTErrorListener=null;
        this.theadDataAccess=null;
        this.JMSQueueName=null;
        this.runMQConnectionFactory=null;
    }
    private String JMSPoint;
    private String JMSLogin;
    private String JMSPasswd;
    private String JMSQueueName;

    public void setJMSPoint(String JMSPoint) {
        this.JMSPoint = JMSPoint;
    }
    public void setJMSQueueName(String JMSQueueName) { this.JMSQueueName = JMSQueueName;}
    public void setJMSPasswd(String JMSPasswd) { this.JMSPasswd = JMSPasswd; }
    public void setJMSLogin(String JMSLogin) { this.JMSLogin = JMSLogin;}
    private String getJMSQueueName() { return this.JMSQueueName; }

    
    public static final Logger JMSReceiveTask_Log = LoggerFactory.getLogger(JMSReceiveTask.class);

    private ActiveMQConnectionFactory MakeActiveMQConnectionFactory(String brokerURL, String pUserName, String pPassword, Logger JMSReceiveTask_Log ) throws JMSException {

        JMSReceiveTask_Log.info("ActiveMQConnectionFactory MsgBus preSet");
        JMSReceiveTask_Log.info("ActiveMQConnectionFactory MsgBus TCP connect: [" + brokerURL + "]");
        String localHostAddress;
        try {
            localHostAddress = InetAddress.getLocalHost().getHostAddress() + "-" + Thread.currentThread().getId();
        } catch (java.net.UnknownHostException e) {
            JMSReceiveTask_Log.warn("InetAddress.getLocalHost().getHostAddress(): " + e.getMessage());
            localHostAddress = "xxx-" + Thread.currentThread().getId();;
        }
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setClientID("JMS.Receiver.Q." + localHostAddress);
        connectionFactory.setClientIDPrefix("ReceiverZ");
        connectionFactory.setMaxThreadPoolSize(50);
        connectionFactory.setAlwaysSyncSend(true);
        connectionFactory.setAlwaysSessionAsync(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setCopyMessageOnSend(false);
        if (pUserName != null) connectionFactory.setUserName(pUserName);
        else connectionFactory.setUserName("");
        if (pPassword != null) connectionFactory.setPassword(pPassword);
        else connectionFactory.setPassword("");

        // Create a Connection Factory

        runMQConnectionFactory = connectionFactory;

        return connectionFactory;
    }

    private int initJMSconnector( String jmsQueueName ) {
    boolean isFaultHappend ;
    for(;;) {
        isFaultHappend =false;
        try {
            if ( this.Qconnection != null)
            {
                if ( this.JMS_Q_Consumer != null)
                {  try  {this.JMS_Q_Consumer.close(); } catch (JMSException e) { JMSReceiveTask_Log.warn( "JMS_Q_Consumer.close()" + e.getMessage() );}
                }
                if ( this.Qsession != null)
                { try { this.Qsession.close(); } catch (JMSException e) { JMSReceiveTask_Log.warn( "Qsession.close()" + e.getMessage() );}
                   this.Qsession = null;
                }
                try { this.Qconnection.stop(); } catch (JMSException e) { JMSReceiveTask_Log.warn( "Qconnection.stop()" + e.getMessage() );}

                try { this.Qconnection.close(); } catch (JMSException e) { JMSReceiveTask_Log.warn( "Qconnection.close()" + e.getMessage() );}

                this.Qconnection = null;
            }
             // TODO !
             this.Qconnection = runMQConnectionFactory.createConnection();


            } catch (JMSException e) {
                    isFaultHappend =true;
                    JMSReceiveTask_Log.error("Ошибка MQconnectionFactory.createConnection() for Interface: " + e.getMessage());
                }
            if ( !isFaultHappend )
                try {
                    // this.Qconnection.setClientID("JMS.Receiver." + Thread.currentThread().getName());  // fault:Setting clientID on a used Connection is not allowed
                    this.Qconnection.start();
                } catch (JMSException e) {
                    isFaultHappend =true;
                    JMSReceiveTask_Log.error("Ошибка Qconnection.start(): " + e.getMessage());
                }
            if ( !isFaultHappend )
                    try {
                    this.Qsession = Qconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    } catch (JMSException e) {
                        isFaultHappend =true;
                        JMSReceiveTask_Log.error("Ошибка Qconnection.createSession(): " + e.getMessage());
                    }
            if ( !isFaultHappend )
                    try {
                    this.Qsession.run();
                    Destination JMSDestination = this.Qsession.createQueue(jmsQueueName);
                    this.JMS_Q_Consumer = this.Qsession.createConsumer(JMSDestination);
            JMSReceiveTask_Log.warn("Удалось подключится к брокеру сообщений ActiveMQ, QueueName:" + jmsQueueName );
            return 0;
        } catch (JMSException e) {
                JMSReceiveTask_Log.error("Ошибка получения Destination createQueue( из Q: " + jmsQueueName + " for Interface: " + e.getMessage());
        }

        try {
            JMSReceiveTask_Log.warn("Пробуем переподключится к брокеру сообщений ActiveMQ, QueueName:" + jmsQueueName + " через 160 сек." );
            Thread.sleep(TimeUnit.SECONDS.toMillis(160));
        } catch (InterruptedException eE ) {
            JMSReceiveTask_Log.error("JMSReceiveTask was Interrupted" + eE.getMessage());
        }

    }
 }
    private void Hermes_DB_Connection_close() {
        if (this.theadDataAccess != null) {
            // -- закрываем соединение с БД
            try {
                if (this.theadDataAccess.Hermes_Connection != null)
                    this.theadDataAccess.Hermes_Connection.close();
                this.theadDataAccess.Hermes_Connection = null;
            } catch (SQLException SQLe) {
                JMSReceiveTask_Log.error(SQLe.getMessage());
                JMSReceiveTask_Log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                SQLe.printStackTrace();
            }
            this.theadDataAccess = null;
        }
    }
public void run()   {
    boolean isDebugged = true;
        //if (( theadNum != null ) && ((theadNum == 17) || (theadNum == 18) || (theadNum == 19) || (theadNum == 20)) )

    JMSReceiveTask_Log.warn( "JMSReceiveTask , ThreadId=" + Thread.currentThread().getId() + " is running, JMSPoint=`" + JMSPoint + "`" );

    // TheadDataAccess
    this.theadDataAccess = new TheadDataAccess();
    // Установаливем " соединение" , что бы зачитывать очередь
    //  theadDataAccess.setDbSchema( ApplicationProperties.HrmsSchema ); - перенесён в make_Hikari_Connection(), что бы не забылось нигде!
    if ( isDebugged )
        JMSReceiveTask_Log.info("Установаливем 'соединение' , что бы зачитывать очередь: [" +
                ApplicationProperties.HrmsPoint + "] user:" + ApplicationProperties.hrmsDbLogin +
                "; passwd:" + ApplicationProperties.hrmsDbPasswd + ".");
    theadDataAccess.make_Hikari_Connection(
            ApplicationProperties.HrmsSchema,
            ApplicationProperties.hrmsDbLogin, ApplicationProperties.hrmsDbPasswd,
            ApplicationProperties.dataSource,
            ApplicationProperties.HrmsPoint,
            JMSReceiveTask_Log
    );
    if ( theadDataAccess.Hermes_Connection == null ){
        JMSReceiveTask_Log.error("Ошибка на инициализации потока приёма JMS-сообщения - theadDataAccess.make_Hikari_Connection('"+ ApplicationProperties.HrmsSchema +"' , ...) return: NULL!"  );
        return;
    }

    //  получить  Interface_id по jmsQueueName из списка Типов для интерфейса, с url_soap_send == jmsQueueName -- DONE
    int MessageTemplateVOkey;
    String jmsQueueName= getJMSQueueName();
    if (jmsQueueName == null) {
        String notify_Error = "Ошибка на инициализации потока приёма JMS-сообщения - имя очереди (JMSReceiveTask.JMSQueueName) ==  NULL!, см. параметр `App_Server` системы";
        JMSReceiveTask_Log.error( notify_Error );
        Hermes_DB_Connection_close();
        NotifyByChannel.Telegram_sendMessage( notify_Error, JMSReceiveTask_Log );
        return;
    }
    int Interface_id = MessageRepositoryHelper.look4MessageTypeVO_2_Interface(jmsQueueName, JMSReceiveTask_Log);
    if (Interface_id <= 0) {
        String notify_Error= "Ошибка на инициализации потока приёма JMS-сообщения не нашли интерфейс для очереди `"+ jmsQueueName+"` - look4MessageTypeVO_2_Interface(), по полю URL_SOAP_Send для поиска Id Interface вернул: " + Interface_id + " !";
        JMSReceiveTask_Log.error( notify_Error);
        Hermes_DB_Connection_close();
        NotifyByChannel.Telegram_sendMessage( notify_Error, JMSReceiveTask_Log );
        return;
    }
    //  -- задержка более не нужена , запуск читателя JMS производится ПОСЛЕ! считывания шаблонов конфигурации

        MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, JMSReceiveTask_Log);
        if (isDebugged)
        JMSReceiveTask_Log.info("Для приёма JMS-сообщения из очереди `"+ jmsQueueName+"` нашли интерфейс look4MessageTemplate_2_Interface(): " + MessageTemplateVOkey + " ");
        JMSReceiveTask_Log.info("Пробуем подключить приёмник JMS-сообщения: Thread[ " + Thread.currentThread().getId() + " ] к интерфейсу ActiveMQ [" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getMsg_Type() + "], jmsQueueName=[" + jmsQueueName + "]");
        // jmsQueueName получен при создании потока getJMSQueueName();
        // jmsQueueName = MessageRepositoryHelper.look4MessageURL_SOAP_Send_by_Interface(Interface_id, JMSReceiveTask_Log); // Q.KIS.EMIAS.XML

      try {
            MakeActiveMQConnectionFactory(
                    JMSPoint,
                    JMSLogin,
                    JMSPasswd,
                    JMSReceiveTask_Log  );
        } catch (JMSException e) {
            String notify_Error ="НЕ удалось подключится к брокеру сообщений ActiveMQ :" + e.getMessage();
            JMSReceiveTask_Log.error( notify_Error );
            Hermes_DB_Connection_close();
            NotifyByChannel.Telegram_sendMessage( notify_Error, JMSReceiveTask_Log );
            return;
        }

        initJMSconnector(jmsQueueName);

        /*
        this.Qconnection = StoreMQpooledConnectionFactory.MQpooledConnectionFactory.createConnection();
        this.Qconnection.setClientID("JMS.Receiver." + Thread.currentThread().getName() );  // fault:Setting clientID on a used Connection is not allowed
        this.Qconnection.start();
        this.Qsession = Qconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination JMSDestination = this.Qsession.createQueue(jmsQueueName);
        this.JMS_Q_Consumer = this.Qsession.createConsumer(JMSDestination);

        try {
            if (isDebugged)
                JMSReceiveTask_Log.info("Пробуем отправить сообщение QUEUE_ID: " + Thread.currentThread().getId() + " в очередь сообщений ActiveMQ[" + jmsQueueName + "]");

            SendTextMessageJMSQueue(
                    "{\"QUEUE_ID\": \"" + Thread.currentThread().getId() + "\"}",
                    JMSDestination,
                    StoreMQpooledConnectionFactory.MQpooledConnectionFactory
            );
        } catch (JMSException e) {
            JMSReceiveTask_Log.warn("НЕ удалось отправить сообщение QUEUE_ID: " + Thread.currentThread().getId() + " в очередь сообщений ActiveMQ[" + jmsQueueName + "], fault:" + e.getMessage());
        }
        if (isDebugged)
            JMSReceiveTask_Log.info("Отправили сообщение QUEUE_ID: " + Thread.currentThread().getId() + " в очередь сообщений ActiveMQ[" + jmsQueueName + "]");
        // Thread.sleep(TimeUnit.SECONDS.toMillis(45) );
        }
    catch (InterruptedException e)
    {
        JMSReceiveTask_Log.error("JMSReceiveTask was Interrupted" + e.getMessage());
        Hermes_DB_Connection_close();
        return;
    } */
    MessageDetails Message = new MessageDetails();
    MessageQueueVO messageQueueVO = new MessageQueueVO();
    if (isDebugged)
        JMSReceiveTask_Log.info("initJMSconnector: " + jmsQueueName + " ok!");

      int theadRunTotalCount = 1;
      for (int theadRunCount = 0; theadRunCount < theadRunTotalCount; theadRunCount += 1 ) {
          try
          {
          Message.Message.clear();
          Message.Confirmation.clear();
          Message.XML_MsgClear.setLength(0); Message.XML_MsgClear.trimToSize();
          Message.Soap_HeaderRequest.setLength(0); Message.Soap_HeaderRequest.trimToSize();
          Message.XML_Request_Method.setLength(0); Message.XML_Request_Method.trimToSize();
          Message.XML_MsgResponse.setLength(0); Message.XML_MsgResponse.trimToSize();
          Message.MsgReason.setLength(0); Message.MsgReason.trimToSize();
          Message.XML_MsgConfirmation.setLength(0); Message.XML_MsgConfirmation.trimToSize();

          // получаем JMS-сообщение из очереди
              if (isDebugged)
                  JMSReceiveTask_Log.info("ReadTextMessageQueue: получаем JMS-сообщение из очереди JMS_Q_Consumer: " + this.JMS_Q_Consumer.toString() );
          Message.XML_MsgConfirmation.append( ReadTextMessageQueue(10 * 1000, isDebugged, JMSReceiveTask_Log));
        if ( Message.XML_MsgConfirmation.length() > 4 ) // != 'null'
        { // не пустое сообщение
            JMSReceiveTask_Log.info("JMS_MsgInput: [" + Message.XML_MsgConfirmation + "], length =" + Message.XML_MsgConfirmation.length() +
                    " Message.XML_MsgConfirmation.substring(0, 1) =[" + Message.XML_MsgConfirmation.substring(0,1) + "]"); // сообщение зачитано

            if (Message.XML_MsgConfirmation.substring(0,1).equals("<")) {
                Message.XML_MsgResponse.append( Message.XML_MsgConfirmation );
            } else {
                if (Message.XML_MsgConfirmation.substring(0, 1).equals("{")) { // Разбираем Json
                    // JSONObject postJSONObject = new JSONObject( Message.XML_MsgConfirmation.toString() );
                    JSONObject RestResponseJSON = new JSONObject(Message.XML_MsgConfirmation.toString());
                    Message.XML_MsgResponse.append(XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse));
                    JMSReceiveTask_Log.info("JSONObject: [" + RestResponseJSON + "]");

                } else { // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]</MsgData>
                    Message.XML_MsgResponse.append(XMLchars.OpenTag + XMLchars.NameRootTagContentJsonResponse + XMLchars.CloseTag + XMLchars.CDATAopen);
                    Message.XML_MsgResponse.append(Message.XML_MsgConfirmation);
                    Message.XML_MsgResponse.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameRootTagContentJsonResponse + XMLchars.CloseTag + XMLchars.CDATAclose);
                }
            }
            Message.XML_MsgInput = Envelope_noNS_Begin
                    + Header_noNS_Begin + Header_noNS_End
                    + Body_noNS_Begin
                    + Message.XML_MsgResponse
                    + Body_noNS_End + Envelope_noNS_End
            ;
            Message.XML_MsgResponse.setLength(0);
            Message.XML_MsgResponse.trimToSize();
            Message.XML_MsgConfirmation.setLength(0);
            Message.XML_MsgConfirmation.trimToSize();
            JMSReceiveTask_Log.info("XML_MsgInput: [" + Message.XML_MsgInput + "]"); // сообщение зачитано

            Long ProcessInputMessageQueue_Result;
            ProcessInputMessageQueue_Result = ProcessInputMessage(Interface_id, Message, MessageTemplateVOkey, messageQueueVO, JMSReceiveTask_Log, isDebugged);

            if (isDebugged) {
                JMSReceiveTask_Log.info("Queue_ID ["+ messageQueueVO.getOutQueue_Id() + "] : XML_MsgResponse => [" + Message.XML_MsgResponse + "], ProcessInputMessageQueue_Result =" + ProcessInputMessageQueue_Result);
                theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, Message.XML_MsgResponse.toString(), JMSReceiveTask_Log);
            }
        }
          theadRunTotalCount += 1;
      }    catch (  JMSException | JSONException e)
          {
              String notify_Error = "Ошибка приёма JMS-сообщения из Q: " + jmsQueueName + " for Interface: " + Interface_id + " returned: "  + e.getMessage() ;
              String localHostName;
              try {
                  localHostName = InetAddress.getLocalHost().getHostName();
              }
              catch (java.net.UnknownHostException UnknownHostE) {
                  localHostName = UnknownHostE.getMessage() + " = set host as 0.0.0.0";
              }
              JMSReceiveTask_Log.error( notify_Error );

              NotifyByChannel.Telegram_sendMessage( localHostName + " " + notify_Error, JMSReceiveTask_Log );
              try {
              Thread.sleep(TimeUnit.SECONDS.toMillis(1));
              }
              catch (InterruptedException InterrupE)
              {
                  JMSReceiveTask_Log.error("Ошибка приёма JMS-сообщения - URL_SOAP_Send for Interface: " + Interface_id + " returned: " + InterrupE.getMessage() );
              }
              // пробуем пересоединиться!
              initJMSconnector(jmsQueueName);

              // Продолжаем работать!
              theadRunTotalCount += 1;
          }

    }
      String notify_Error = "JMSReceiveTask is finished, JMSPoint =" + JMSPoint ;
        JMSReceiveTask_Log.error( notify_Error );
    NotifyByChannel.Telegram_sendMessage( notify_Error, JMSReceiveTask_Log );
      // Если уж выходим, освобождаем DB_Connection
    Hermes_DB_Connection_close();
        return;
}
/*
    public void SendTextMessageJMSQueue(String TextMessageSring, Destination JMSdestination , PooledConnectionFactory MQpooledConnectionFactory ) throws JMSException {

        // We will send a small text message saying 'Hello World!!!'
        MessageProducer producer = this.Qsession.createProducer(JMSdestination);
        TextMessage message = this.Qsession.createTextMessage( TextMessageSring );
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        ////////////////////////////////////
        //Destination tempDestResponce = this.Qsession.createTemporaryQueue();
        //message.setJMSReplyTo( tempDestResponce );

        //this.JMS_Q_Consumer = this.Qsession.createConsumer(tempDestResponce);
        producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
        producer.send(message);
        producer.close();

        return ;
    }
*/
    public String ReadTextMessageQueue ( int TimeOut, boolean IsDebugged, Logger MessegeReceive_Log) throws JMSException {
        // JMSconsumer надо читать с блокировкой
        if ( this.JMS_Q_Consumer != null) {

            TextMessage JMSTextMessage = (TextMessage) this.JMS_Q_Consumer.receive(TimeOut * 1);
            if (JMSTextMessage != null) {
                String JMSMessageID = JMSTextMessage.getJMSMessageID();
                if (IsDebugged)
                    MessegeReceive_Log.info("Received message: (" + JMSMessageID + ") [" + JMSTextMessage.getText() + "]");
                return JMSTextMessage.getText();
            }
        }
        return null;
    }

    public Long  ProcessInputMessage( Integer Interface_id , MessageDetails Message, // контейнер сообщения для обработкиMessage.XML_MsgInput - заполнен входящими данными <Envelope/>
                                      int MessageTemplateVOkey, // индекс интерфейсного Шаблона
                                      MessageQueueVO messageQueueVO,
                                      Logger MessegeReceive_Log,
                                      boolean isDebugged) {

        XSLTErrorListener = new xlstErrorListener();
        StringBuilder ConvXMLuseXSLTerr = new StringBuilder(); ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        XSLTErrorListener.setXlstError_Log( MessegeReceive_Log );
        final  String Queue_Direction="ProcessInputMessage";
        Long Queue_Id = -1L;
        Long Function_Result = 0L;

        // MessageTemplateVOkey - Шаблон интерфейса (на основе входного URL)
        try {
            if ( MessageTemplateVOkey >= 0 )
                // Парсим входной запрос и формируем XML-Document !
                XMLutils.makeClearRequest(Message, MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getEnvelopeInXSLT(),
                        XSLTErrorListener, ConvXMLuseXSLTerr, isDebugged,
                        MessegeReceive_Log);
            else
                XMLutils.makeClearRequest(Message, null,
                        XSLTErrorListener, ConvXMLuseXSLTerr, isDebugged,
                        MessegeReceive_Log);
        }
        catch (Exception e) {
            // System.err.println( "["+ Message.XML_MsgInput + "]  Exception" );
            // e.printStackTrace();
            MessegeReceive_Log.error(Queue_Direction + "fault: [" + Message.XML_MsgInput + "] XMLutils.makeClearRequest fault: " + sStackTracе.strInterruptedException(e));
            Message.MsgReason.append("Ошибка на приёме сообщения: " + e.getMessage() ); //  sStackTracе.strInterruptedException(e));
            if ( (e instanceof JDOMParseException) || (e instanceof XPathExpressionException)  ||( e instanceof SAXParseException ) ) // Клиент прислсл фуфло
                return 1L;
            else
                return -1L;

        }
        if ( isDebugged )
            MessegeReceive_Log.info("Clear request:" + Message.XML_MsgClear.toString() );


        // Создаем запись в таблице-очереди  select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL ...
        Queue_Id = MessageUtils.MakeNewMessage_Queue( messageQueueVO, theadDataAccess, MessegeReceive_Log );
        if ( Queue_Id == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения, не удалось сохранить заголовок сообщения в БД - MakeNewMessage_Queue return: " + Queue_Id );
            return -3L;
        }
        Message.ROWID_QUEUElog=null; Message.Queue_Id = Queue_Id;
        if ( isDebugged )
            Message.ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( Queue_Id , Message.XML_MsgInput, MessegeReceive_Log );


        if ( MessageTemplateVOkey >= 0 )
        {  // Получаем Шаблон формирования заголовка для этого интерфейса HeaderInXSLT
            String MessageXSLT_4_HeaderIn = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getHeaderInXSLT();
            if ( isDebugged )
                MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "] MessageXSLT_4_HeaderIn= MessageTemplate.AllMessageTemplate.get{" + MessageXSLT_4_HeaderIn+ "}");
            // При наличии в шаблоне  интерфейса файла преобразования HeaderInXSLT,
            //  к исходному запросу(SOAP-Envelope) с удаленной информацией о NameSpace
            //  применяется преобразование HeaderInXSLT и полученный результат
            //  заменяет SOAP-Header запроса, вне зависимости от того был он или нет.
            if ( MessageXSLT_4_HeaderIn != null )
            {
                ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
                try {
                    Message.Soap_HeaderRequest.append(XMLutils.ConvXMLuseXSLT(Queue_Id,
                            Message.XML_MsgClear.toString(),
                            MessageXSLT_4_HeaderIn,
                            Message.MsgReason,
                            ConvXMLuseXSLTerr,
                            XSLTErrorListener,
                            MessegeReceive_Log,
                            isDebugged
                            ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    //if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    if ( isDebugged )
                        MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    if ( Message.Soap_HeaderRequest.toString().equals(XMLchars.nanXSLT_Result) ) {
                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess, "В результате XSLT преобразования получен пустой заголовок из (" + Message.XML_MsgClear.toString() + ")",
                                null, MessegeReceive_Log);
                        Message.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                        return -5L;
                    }

                } catch (TransformerException exception) {
                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_HeaderIn + "}");
                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] fault " + ConvXMLuseXSLTerr.toString() + " после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " + ConvXMLuseXSLTerr + Message.XML_MsgClear.toString() + " on " + MessageXSLT_4_HeaderIn,
                            null, MessegeReceive_Log);
                    Message.MsgReason.append("Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " +  ConvXMLuseXSLTerr);
                    // Считаем, что виноват клиент
                    return 5L;
                }

                try { // Парсим заголовок - получаем атрибуты messageQueueVO для сохранения в БД
                    XMLutils.Soap_HeaderRequest2messageQueueVO(Message.Soap_HeaderRequest.toString(), messageQueueVO, MessegeReceive_Log);

                }
                catch (Exception e) {
                    System.err.println( "Queue_Id["+ messageQueueVO.getQueue_Id() + "]  Exception" );
                    e.printStackTrace();
                    MessegeReceive_Log.error(Queue_Direction + "fault: [" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: " + sStackTracе.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString() + ", fault: " + sStackTracе.strInterruptedException(e));

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString(),
                            e, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 7L;
                }

            }
            else { // Берем распарсенный  Context из Header сообщения для сохранения в БД
                if ( Message.Input_Header_Context != null ) {
                    try {
                        XMLutils.Soap_XMLDocument2messageQueueVO(Message.Input_Header_Context, messageQueueVO, MessegeReceive_Log);
                    } catch (Exception e) {
                        System.err.println("Queue_Id [" + messageQueueVO.getQueue_Id() + "]  Exception");
                        e.printStackTrace();
                        MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTracе.strInterruptedException(e));
                        Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Queue_Direction + ", fault: " + sStackTracе.strInterruptedException(e));

                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                                "Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                                e, MessegeReceive_Log);
                        // Считаем, что виноват клиент
                        return 9L;

                    }
                }
                else {
                    MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_XMLDocument2messageQueueVO: (" +  Message.XML_MsgClear + ") fault " );
                    Message.MsgReason.append("Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения (: " + Message.XML_MsgClear.toString() + ")" );

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                            null, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 11L;
                }
            }

        }
        else { // Шаблона на Интерфейсе нет!
            // Берем распарсенный  Context из Header сообщения для сохранения в БД
            if ( Message.Input_Header_Context != null ) {
                try {
                    XMLutils.Soap_XMLDocument2messageQueueVO(Message.Input_Header_Context, messageQueueVO, MessegeReceive_Log);
                } catch (Exception e) {
                    System.err.println("Queue_Id [" + messageQueueVO.getQueue_Id() + "]  Exception");
                    e.printStackTrace();
                    MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTracе.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: ");
                    Message.MsgReason.append(Queue_Direction);
                    Message.MsgReason.append(", fault: ");Message.MsgReason.append( sStackTracе.strInterruptedException(e));

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                            e, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 9L;

                }
            }
            else {
                MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_XMLDocument2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " );
                Message.MsgReason.append("Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения (: " + Message.XML_MsgClear.toString() + ")" );

                MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                        "Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                        null, MessegeReceive_Log);
                // Считаем, что виноват клиент
                return 11L;
            }
        }

        Message.XML_MsgResponse.setLength(0); Message.XML_MsgResponse.trimToSize();
            // создание Http-клиента перенеено в PerfotmInputMessagesюperformMessage()
        PerfotmInputMessages Perfotmer = new PerfotmInputMessages();
        Message.ReInitMessageDetails() ; // sslContext, httpClientBuilder, null, ApiRestHttpClient );
        try {

            // Обрабатываем сообщение!
            Function_Result = Perfotmer.performMessage(Message, messageQueueVO, theadDataAccess,
                    XSLTErrorListener,  ConvXMLuseXSLTerr,  MessegeReceive_Log );

        }
        catch (Exception e) {
            System.err.println( "performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            e.printStackTrace();
            MessegeReceive_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            MessegeReceive_Log.error( "что то пошло совсем не так...");
            MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                    "Perfotmer.performMessage fault:"  + e.getMessage() + ", " + Message.XML_MsgClear.toString()  ,
                    null ,  MessegeReceive_Log);
            /*  создание Http-клиента перенеено в PerfotmInputMessagesюperformMessage()
            try {
                ApiRestHttpClient.close();
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( IOException IOe) {
                MessegeReceive_Log.error( "И ещё проблема с ApiRestHttpClient.close()/syncConnectionManager.shutdown()...");
                IOe.printStackTrace();
            }
            return -11L;
             */
        }
/*    try {
            ApiRestHttpClient.close();
            syncConnectionManager.shutdown();
            syncConnectionManager.close();
        } catch ( IOException e) {
            MessegeReceive_Log.error( "И ещё проблема с ApiRestHttpClient.close()...");
            e.printStackTrace();
        }
*/

/*
            for ( theadRunCount = 0; theadRunCount < theadRunTotalCount; theadRunCount += 1 ) {
                long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
                if ( secondsFromEpoch - startTimestamp > Long.valueOf(60L * TotalTimeTasks) )
                    break;
                else
                    theadRunTotalCount += 1;
                try {
                    try {
                        ResultSet rs = stmtMsgQueue.executeQuery();
                        while (rs.next()) {
                            messageQueueVO.setMessageQueue(
                                    rs.getLong("Queue_Id"),
                                    rs.getLong("Queue_Date"),
                                    rs.getLong("OutQueue_Id"),
                                    rs.getLong("Msg_Date"),
                                    rs.getInt("Msg_Status"),
                                    rs.getInt("MsgDirection_Id"),
                                    rs.getInt("Msg_InfoStreamId"),
                                    rs.getInt("Operation_Id"),
                                    rs.getString("Queue_Direction"),
                                    rs.getString("Msg_Type"),
                                    rs.getString("Msg_Reason"),
                                    rs.getString("Msg_Type_own"),
                                    rs.getString("Msg_Result"),
                                    rs.getString("SubSys_Cod"),
                                    rs.getString("Prev_Queue_Direction"),
                                    rs.getInt("Retry_Count"),
                                    rs.getLong("Prev_Msg_Date"),
                                    rs.getLong("Queue_Create_Date")
                            );

                            MessegeReceive_Log.info( "messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") +
                                    " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod"));
                            // Очистили Message от всего, что там было
                            Message.ReInitMessageDetails( sslContext, httpClientBuilder, null, ApiRestHttpClient );
                            try {
                                PerfotmOUTMessages.performChildOutMessage(Message, messageQueueVO, TheadDataAccess, MessegeReceive_Log );
                            }
                            catch (Exception e) {
                                System.err.println( "performMessage Exception Queue_Id:[" + rs.getLong("Queue_Id") + "] " +e.getMessage());
                                e.printStackTrace();
                                MessegeReceive_Log.error("performMessage Exception Queue_Id:[" + rs.getLong("Queue_Id") + "] " +e.getMessage());
                                MessegeReceive_Log.error( "что то пошло совсем не так...");
                            }
                        }
                        rs.close();
                    } catch (Exception e) {
                        MessegeReceive_Log.error(e.getMessage());
                        e.printStackTrace();
                        MessegeReceive_Log.error( "что то пошло совсем не так...");
                        return;
                    }

                    MessegeReceive_Log.info( "Ждём'c; в " + theadRunCount + " раз " +  WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp ) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch
                            // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
                    );
                    Thread.sleep( WaitTimeBetweenScan* 1000);

                } catch (InterruptedException e) {
                    MessegeReceive_Log.error("MessageSendTask[" + theadNum + "]: is interrapted: " + e.getMessage());
                    e.printStackTrace();
                }

             //   MessegeReceive_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");

            }
            */
        // MessegeReceive_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
        // this.SenderExecutor.shutdown();

        return Function_Result.longValue(); // messageQueueVO.getQueue_Id();
    }

}
