package net.plumbing.msgbus.controller;

import net.plumbing.msgbus.threads.TheadDataAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;

import net.plumbing.msgbus.mq.StoreMQpooledConnectionFactory;
import net.plumbing.msgbus.mq.PerformTextMessageJMSQueue;

import javax.jms.JMSException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PushMessage2ActiveMQ {
    public static final Logger MessegeReceive_Log = LoggerFactory.getLogger(PushMessage2ActiveMQ.class);
    public TheadDataAccess theadDataAccess=null;


    public Long push_QeueId2ActiveMQqueue( long pQueue_ID, StringBuilder MsgReason )  {
        Long Queue_Id = -1L;
        Long Function_Result = 0L;
        boolean isDebugged = true;

        // TheadDataAccess
        this.theadDataAccess = new TheadDataAccess();
        // Установаливем " соединение" , что бы зачитывать очередь
        if ( isDebugged )
            MessegeReceive_Log.info("Установаливем \"соединение\" , что бы зачитывать очередь: [" +
                    ApplicationProperties.HrmsPoint + "] user:" + ApplicationProperties.hrmsDbLogin +
                    "; passwd:" + ApplicationProperties.hrmsDbPasswd + ".");

        theadDataAccess.make_Hikari_Connection_Only(
                ApplicationProperties.hrmsDbLogin, ApplicationProperties.hrmsDbPasswd,
                ApplicationProperties.dataSource,
                MessegeReceive_Log
        );
        if ( theadDataAccess.Hermes_Connection == null ){
            MsgReason.append("Ошибка на приёме сообщения - theadDataAccess.make_Hikari_Connection return: NULL!"  );
            return -2L;
        }

        PreparedStatement stmtQueueLock4JMSconsumer;
        ResultSet rLock = null;
        String Queue_Direction=null;
        Integer theadNum = null;
        try {
        stmtQueueLock4JMSconsumer = theadDataAccess.Hermes_Connection.prepareStatement( "select Q.ROWID, q.Queue_Direction, q.msg_InfostreamId from ARTX_PROJ.MESSAGE_QUEUE q where q.Queue_Id=? " );
            stmtQueueLock4JMSconsumer.setLong(1, pQueue_ID );
            rLock = stmtQueueLock4JMSconsumer.executeQuery();
            while (rLock.next()) {
                Queue_Direction = rLock.getString("Queue_Direction");
                theadNum = rLock.getInt("msg_InfostreamId");
                MessegeReceive_Log.info( "[" + pQueue_ID +"] push_QeueId2ActiveMQqueue: Queue_Id="+ pQueue_ID+ " :" +
                        " record readed. msg_InfostreamId=" + theadNum + " Queue_Direction=[" + Queue_Direction + "]");
                Queue_Id = pQueue_ID;
            }
        } catch (Exception e) {
            e.printStackTrace();
            MessegeReceive_Log.error("[" + pQueue_ID +"] Ошибка на приёме сообщения: 'select Q.ROWID, q.Queue_Direction, q.msg_InfostreamId from ARTX_PROJ.MESSAGE_QUEUE q where q.Queue_Id ="+ pQueue_ID + "' fault:" + e.getMessage() );
            MsgReason.append("Ошибка на приёме сообщения: 'select Q.ROWID, q.Queue_Direction, q.msg_InfostreamId from ARTX_PROJ.MESSAGE_QUEUE q where q.Queue_Id ="+ pQueue_ID + "' fault:" + e.getMessage()  );
            return -3L;
        }
        String MessageDirectionsCode = null;
        if ( theadNum != null )
         MessageDirectionsCode = MessageRepositoryHelper.look4MessageDirectionsCode_4_Num_Thread( theadNum  , MessegeReceive_Log );
        else  {
            MessegeReceive_Log.error("[" + pQueue_ID +"] push_QeueId2ActiveMQqueue:НЕ удалось Найти  № потока по Queue_Id ="+ pQueue_ID + " он нужен для роиска очереди сообщений ActiveMQ");
            MsgReason.append("НЕ удалось Найти  № потока по Queue_Id ="+ pQueue_ID + " он нужен для роиска очереди сообщений ActiveMQ");
            return Queue_Id;
        }

        if ( MessageDirectionsCode == null )
        {
            try {
                theadDataAccess.Hermes_Connection.close();
            } catch (SQLException e) { MessegeReceive_Log.error("[" + pQueue_ID +"] push_QeueId2ActiveMQqueue: Hermes_Connection.close() fault ="+ e.getMessage());
                e.printStackTrace();
            }
            MessegeReceive_Log.error("НЕ удалось Найти подходящйю систему для № потока "+ theadNum + " она нужна для очереди сообщений ActiveMQ");
            MsgReason.append("НЕ удалось Найти подходящйю систему для № потока "+ theadNum + " она нужна для очереди сообщений ActiveMQ");
            return Queue_Id;
        }
        PerformTextMessageJMSQueue performTextMessageJMSQueue = new PerformTextMessageJMSQueue();
        javax.jms.Connection Qconnection = null;
        try {
            MessegeReceive_Log.info("[" + pQueue_ID +"] Пробуем отправить сообщение QUEUE_ID: "+ Queue_Id + " в очередь сообщений ActiveMQ \'Q." + MessageDirectionsCode + ".IN'");
             Qconnection = performTextMessageJMSQueue.SendTextMessageJMSQueue(
                    "{ \"QUEUE_ID\": \"" + Queue_Id.toString() + "\" }",
                    "Q." + MessageDirectionsCode + ".IN",
                    StoreMQpooledConnectionFactory.MQpooledConnectionFactory
            );
        } catch (JMSException e) {
            MessegeReceive_Log.error("[" + pQueue_ID +"] НЕ удалось отправить сообщение QUEUE_ID: "+ Queue_Id + " в очередь сообщений ActiveMQ \'Q." + MessageDirectionsCode + ".IN', fault:" + e.getMessage());
            MsgReason.append("НЕ удалось отправить сообщение QUEUE_ID: "+ Queue_Id + " в очередь сообщений ActiveMQ \'Q." + MessageDirectionsCode + ".IN':" + e.getMessage());
            return Queue_Id;

        }
        MsgReason.append("push_QeueId2ActiveMQqueue: Queue_Id="+ pQueue_ID+ " :" +
                " record readed. msg_InfostreamId=" + theadNum + " отправлено в очередь сообщений ActiveMQ \'Q." + MessageDirectionsCode + ".IN'");
        return Queue_Id;
    }
}
