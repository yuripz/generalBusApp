package net.plumbing.msgbus.threads.utils;

// import org.apache.commons.lang3.StringEscapeUtils;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageDetailVO;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.text.StringEscapeUtils;
import org.jdom2.*;
//import org.jdom2.filter.Filters;
//import org.jdom2.input.SAXBuilder;
//import org.jdom2.xpath.XPathExpression;
//import org.jdom2.xpath.XPathFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
//import MessageDirections;

//import javax.validation.constraints.NotNull;
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static net.plumbing.msgbus.controller.MessageReceiveTask.MessegeReceive_Log;
public class MessageUtils {

    // static  Long Tag_Num = new  Long(0L);
/*
    public static String MakeEntryOutHeader(MessageQueueVO messageQueueVO, int MsgDirectionVO_Key) {
        return ("<" + XMLchars.TagEntryRec + ">"+
                "<" + XMLchars.TagEntryInit + ">" + XMLchars.HermesMsgDirection_Cod + "</" + XMLchars.TagEntryInit + ">" +
                "<" + XMLchars.TagEntryKey + ">" + messageQueueVO.getQueue_Id() + "</" + XMLchars.TagEntryKey + ">"+
                "<" + XMLchars.TagEntrySrc + ">" + XMLchars.HermesMsgDirection_Cod + "</" + XMLchars.TagEntrySrc + ">"+
                "<" + XMLchars.TagEntryDst + ">" + MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Cod() + "</" + XMLchars.TagEntryDst + ">" +
                "<" + XMLchars.TagEntryOpId + ">" + messageQueueVO.getOperation_Id() + "</" + XMLchars.TagEntryOpId + ">" +
                "<" + XMLchars.TagOutIdKey + ">" + messageQueueVO.getOutQueue_Id() + "</" + XMLchars.TagOutIdKey + ">" +
                "</" + XMLchars.TagEntryRec + ">"
        );
    }
    public static Long MakeNewRestApi_Queue( MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessegeReceive_Log ){
        ResultSet rs = null;
        try {
            rs = theadDataAccess.stmt_New_Queue_Prepare.executeQuery();
            while (rs.next()) {
                messageQueueVO.setMessageQueue(
                        rs.getLong("Queue_Id"),
                        rs.getTimestamp("Queue_Date"),
                        rs.getLong("OutQueue_Id"),
                        rs.getTimestamp("Msg_Date"),
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
                        rs.getTimestamp("Prev_Msg_Date"),
                        rs.getTimestamp("Queue_Create_Date"),
                        0L
                );
            }
            rs.close();
        }
        catch (SQLException e)
        {
            MessegeReceive_Log.error(e.getMessage());
            e.printStackTrace();
            MessegeReceive_Log.error( "что то пошло совсем не так...:" + theadDataAccess.selectMessageStatement);
            //if ( rs !=null ) rs.close();
            return null;
        }

        Long Queue_Id = messageQueueVO.getQueue_Id();
        try {
            // "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) "
            theadDataAccess.stmt_New_Queue_Insert.setLong(1, Queue_Id);
            theadDataAccess.stmt_New_Queue_Insert.executeUpdate();
            MessegeReceive_Log.info(  ">" + theadDataAccess.INSERT_Message_Queue + ":Queue_Id=[" + Queue_Id + "] done");


        } catch (SQLException e) {
            MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Queue + ":Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return null;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("Hermes_Connection.commit() fault: " + exp.getMessage());
            return null;
        }
        return Queue_Id;
    }
*/
    public static Long MakeNewMessage_Queue(MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessegeReceive_Log ){
        ResultSet rs = null;
        try {
            rs = theadDataAccess.stmt_New_Queue_Prepare.executeQuery();
            while (rs.next()) {
                messageQueueVO.setMessageQueue(
                        rs.getLong("Queue_Id"),
                        rs.getTimestamp("Queue_Date"),
                        rs.getString("OutQueue_Id"),
                        rs.getTimestamp("Msg_Date"),
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
                        rs.getTimestamp("Prev_Msg_Date"),
                        rs.getTimestamp("Queue_Create_Date"),
                        0L
                );
            }
            rs.close();
            theadDataAccess.Hermes_Connection.commit();
        }
        catch (SQLException e)
        {
            MessegeReceive_Log.error(e.getMessage());
            System.err.println("Queue_Id=[ NewMessage_Queue ] :" + e.getMessage() );
            e.printStackTrace();
            MessegeReceive_Log.error( "что то пошло совсем не так...:" + theadDataAccess.selectMessageStatement);
            //if ( rs !=null ) rs.close();
            return null;
        }

         long Queue_Id = messageQueueVO.getQueue_Id();
        try {
                // "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) "
                theadDataAccess.stmt_New_Queue_Insert.setLong(1, Queue_Id);
                theadDataAccess.stmt_New_Queue_Insert.executeUpdate();
            // MessegeReceive_Log.info(  ">" + theadDataAccess.INSERT_Message_Queue + ":Queue_Id=[" + Queue_Id + "] done");

        } catch (SQLException e) {
            MessegeReceive_Log.error("["+ Queue_Id +"] MakeNewMessage_Queue `" + theadDataAccess.INSERT_Message_Queue + "` fault: " + sStackTrace.strInterruptedException(e));
            System.err.println("["+ Queue_Id +"] MakeNewMessage_Queue `" + theadDataAccess.INSERT_Message_Queue + "` fault: " + e.getMessage());
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return null;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("["+ Queue_Id +"] MakeNewMessage_Queue Hermes_Connection.commit()  fault: " + sStackTrace.strInterruptedException(exp));
            return null;
        }
        return Queue_Id;
    }

    public static String PrepareEnvelope4XSLTPost( MessageQueueVO messageQueueVO, StringBuilder XML_MsgConfirmation) {
        //int nn = 0;
        StringBuilder SoapEnvelope = new StringBuilder(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        // SoapEnvelope.append("<MsgId>" + messageQueueVO.getQueue_Id() +"</MsgId>");
        SoapEnvelope.append( XMLchars.MsgId_Begin ).append( messageQueueVO.getQueue_Id()).append(  XMLchars.MsgId_End );
        SoapEnvelope.append(XMLchars.Header_noNS_End);

        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( XMLchars.MsgId_Begin ).append( messageQueueVO.getQueue_Id()).append(  XMLchars.MsgId_End );
        if ( XML_MsgConfirmation != null) SoapEnvelope.append(XML_MsgConfirmation);

        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);

        return SoapEnvelope.toString();
    }
/*
    public static Integer ProcessingSendError(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                              String whyIsFault , boolean isMessageQueue_Directio_2_ErrorOUT, Exception e , Logger MessegeReceive_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTrace.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";


        int messageRetry_Count = messageQueueVO.getRetry_Count();
        messageRetry_Count += 1; // увеличили счетчик попыток
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getShortRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1236,
                    messageRetry_Count, MessegeReceive_Log
            );
            return -1;
        }
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getLongRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1237,
                    messageRetry_Count, MessegeReceive_Log
            );
            return -1;
        }
        if ( isMessageQueue_Directio_2_ErrorOUT ) // Если это не Транспортная ошибка, то выставляем ERROROUT
        {
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO.getQueue_Id(),
                    whyIsFault + " fault: " + ErrorExceptionMessage, 1239,
                    messageQueueVO.getRetry_Count(), MessegeReceive_Log);
            messageQueueVO.setQueue_Direction(XMLchars.DirectERROUT);
        }
        return -1;
    }
*/
    public static Integer ProcessingIn_setIN(MessageQueueVO messageQueueVO,  TheadDataAccess theadDataAccess,
                                             Logger MessegeReceive_Log)
    {

        MessegeReceive_Log.warn( messageQueueVO.toSring() );
        int result = theadDataAccess.doUPDATE_MessageQueue_In2Ok(messageQueueVO.getQueue_Id(),
                                                                 messageQueueVO.getOperation_Id(),
                messageQueueVO.getMsgDirection_Id(), messageQueueVO.getSubSys_Cod(),
                messageQueueVO.getMsg_Type(), messageQueueVO.getMsg_Type_own(),
                messageQueueVO.getMsg_Reason() != null ? messageQueueVO.getMsg_Reason(): "-", // Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason
                messageQueueVO.getOutQueue_Id(),
                MessegeReceive_Log);
        messageQueueVO.setQueue_Direction(XMLchars.DirectIN);
        return result;
    }

    public static Integer ProcessingIn2ErrorIN(MessageQueueVO messageQueueVO, MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                                 String whyIsFault , Exception e , Logger MessegeReceive_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTrace.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";

        int result = theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(messageQueueVO.getQueue_Id(),
                whyIsFault + " fault: " + ErrorExceptionMessage,
                 3100, MessegeReceive_Log);
        messageQueueVO.setQueue_Direction(XMLchars.DirectERRIN);
        return result;
    }

    public static String PrepareEnvelope4XSLTExt(MessageQueueVO messageQueueVO, StringBuilder XML_Request_Method, Logger MessegeReceive_Log) {
        // Искуственный Envelope/Head/Body + XML_Request_Method
        int nn = XML_Request_Method.length() +
                2 * (XMLchars.Envelope_noNS_End.length() + XMLchars.Header_noNS_End.length() + XMLchars.MsgId_End.length() + XMLchars.Body_noNS_End.length() )
                 + 24;
        StringBuilder SoapEnvelope = new StringBuilder( nn );
        SoapEnvelope.append(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        SoapEnvelope.append(XMLchars.MsgId_Begin + messageQueueVO.getQueue_Id() + XMLchars.MsgId_End);
        SoapEnvelope.append(XMLchars.Header_noNS_End);

        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( XML_Request_Method );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);
        MessegeReceive_Log.warn( "PrepareEnvelope4XSLTExt: {"+ SoapEnvelope.toString() + "}" );
        return SoapEnvelope.toString();
    }

    public static int ReadMessage(TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, boolean IsDebugged, Logger MessegeSend_Log) {
        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;

        Pattern pattern = Pattern.compile("\\d+");
        try {
            theadDataAccess.stmtMsgQueueDet.setLong(1, Queue_Id);
            ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
            String rTag_Value=null;
            String rTag_Id=null;
            String xmlTag_Id;
            int PrevTag_Par_Num=-102030405;
            int rTag_Num;
            int rTag_Par_Num;
            ArrayList<Integer> ChildVO_ArrayList= new ArrayList<Integer>();

            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                rTag_Value = rs.getString("Tag_Value");
                rTag_Num= rs.getInt("Tag_Num");
                rTag_Par_Num = rs.getInt("Tag_Par_Num");
                rTag_Id= rs.getString("Tag_Id");
                if ( pattern.matcher( rTag_Id ).matches() )
                    xmlTag_Id = "Element" + rTag_Id;
                else
                    xmlTag_Id = rTag_Id;
                if ( rTag_Value == null )
                    messageDetailVO.setMessageQueue( xmlTag_Id, null, rTag_Num,rTag_Par_Num);
                else
                    messageDetailVO.setMessageQueue( xmlTag_Id,
                            org.apache.commons.lang3.StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                            rTag_Num,rTag_Par_Num);

                messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);

                // для получения messageDetails.Message.get( messageDetails.MessageIndex_by_Tag_Par_Num.get(Tag_Par_Num) )
                if (PrevTag_Par_Num != rTag_Par_Num) { // получили элемент у которого другой "пара"
                    if (PrevTag_Par_Num != -102030405 ) {
                        //MessegeSend_Log.warn("PrevTag_Par_Num[" + PrevTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());
                        ChildVO_ArrayList = new ArrayList<Integer>();
                    }

                    messageDetails.MessageIndex_by_Tag_Par_Num.put( rTag_Par_Num, ChildVO_ArrayList);
                    PrevTag_Par_Num = rTag_Par_Num;
                }
                else {
                    ChildVO_ArrayList = messageDetails.MessageIndex_by_Tag_Par_Num.get(rTag_Par_Num);
                    //MessegeSend_Log.warn("rTag_Par_Num[" + rTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());
                }
                ChildVO_ArrayList.add( messageDetails.MessageRowNum );
                //MessegeSend_Log.warn("add to Tag_Par_Num[" + rTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());

                messageDetails.MessageRowNum += 1;
                if ( messageDetails.MessageRowNum % 10000 == 0)
                    MessegeSend_Log.info( "["+ Queue_Id +"] читаем из БД тело XML, " + messageDetails.MessageRowNum + " записей" );
                // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
            }
        } catch (SQLException e) {
            MessegeSend_Log.error("Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            System.err.println("["+ Queue_Id +"] select  from MESSAGE_QUEUEdet  fault: " + e.getMessage());
            e.printStackTrace();
            return messageDetails.MessageRowNum;
        }
        MessegeSend_Log.info( "["+ Queue_Id +"] считали из БД фрагменты XML, " + messageDetails.MessageRowNum + " записей" );

        if ( messageDetails.MessageRowNum > 0 )
            try {
                XML_Current_Tags( messageDetails, 0);
            } catch ( NullPointerException e ) {
                // NPE случилось, печатаем диагностику
                MessegeSend_Log.warn("[" + Queue_Id + "] проверяем вторчный индекс MessageIndex_by_Tag_Par_Num, потому как получили NullPointerException на подготовке XML" );
                Set<Integer> MessageIndexSet = messageDetails.MessageIndex_by_Tag_Par_Num.keySet();
                Iterator MessageIndexIterator = MessageIndexSet.iterator();
                while (MessageIndexIterator.hasNext()) {
                    Integer i = (Integer) MessageIndexIterator.next();
                    MessegeSend_Log.warn("[" + Queue_Id + "] MessageIndex_by_Tag_Par_Num[" + i + "]" +
                            messageDetails.MessageIndex_by_Tag_Par_Num.get(i).toString());
                }
                MessageIndexSet = messageDetails.Message.keySet();
                Iterator messageDetailsIterator = MessageIndexSet.iterator();
                while (messageDetailsIterator.hasNext()) {
                    Integer i = (Integer) messageDetailsIterator.next();
                    MessegeSend_Log.warn("[" + Queue_Id + "] messageDetails.Message[" + i + "] <" +
                            messageDetails.Message.get(i).Tag_Id + ">" +
                            messageDetails.Message.get(i).Tag_Value +
                            "; Tag_Num=" + messageDetails.Message.get(i).Tag_Num +
                            "; Tag_Par_Num=" + messageDetails.Message.get(i).Tag_Par_Num
                    );
                }
                MessegeSend_Log.info( "["+ Queue_Id +"] тело XML тело XML не получено из БД , остановлено на " + messageDetails.XML_MsgResponse.length() + " символов" );
            }

        MessegeSend_Log.info( "["+ Queue_Id +"] получили из БД тело XML, " + messageDetails.XML_MsgResponse.length() + " символов" );
        if (IsDebugged ) {
            MessegeSend_Log.info(messageDetails.XML_MsgResponse.toString());
        }
        return messageDetails.MessageRowNum;
    }

    // @messageDetails.XML_MsgResponse формируется из messageDetails.Message
    public static int XML_Current_Tags(@NotNull MessageDetails messageDetails, int Current_Elm_Key) throws UnsupportedOperationException, NullPointerException  {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(Current_Elm_Key);


        if ( messageDetailVO.Tag_Num != 0 ) { // Tag_Num Всегда начинается с 1 для сообщения! ( проверка на всякий случай )

            messageDetails.XML_MsgResponse.append(XMLchars.OpenTag ); messageDetails.XML_MsgResponse.append( messageDetailVO.Tag_Id);

            MessageDetailVO messageChildVO;
            ArrayList<Integer> ChildVO_ArrayList =  messageDetails.MessageIndex_by_Tag_Par_Num.get( messageDetailVO.Tag_Num );
            if (ChildVO_ArrayList != null ) { // у элемента <messageDetailVO.Tag_Id ...> есть либо атрибуты, либо вложенные элементы
                Iterator<Integer> ChildVO_AttributeIterator = ChildVO_ArrayList.iterator();
                // 1й проход, достаём атрибуты элемента
                while (ChildVO_AttributeIterator.hasNext()) {
                    Integer i = ChildVO_AttributeIterator.next();
                    messageChildVO = messageDetails.Message.get(i);
                    if ( messageChildVO != null) {
                        if ((messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                                (messageChildVO.Tag_Num == 0))  // это атрибут элемента, у которого нет потомков
                        {
                            if (messageChildVO.Tag_Value != null) { // фармирукм Attribute="Value"
                                messageDetails.XML_MsgResponse.append(XMLchars.Space);
                                messageDetails.XML_MsgResponse.append(messageChildVO.Tag_Id);
                                messageDetails.XML_MsgResponse.append(XMLchars.Equal);
                                messageDetails.XML_MsgResponse.append(XMLchars.Quote);
                                messageDetails.XML_MsgResponse.append(messageChildVO.Tag_Value);
                                messageDetails.XML_MsgResponse.append(XMLchars.Quote);
                            } else {  // фармирукм Attribute=""
                                messageDetails.XML_MsgResponse.append(XMLchars.Space);
                                messageDetails.XML_MsgResponse.append(messageChildVO.Tag_Id);
                                messageDetails.XML_MsgResponse.append(XMLchars.Equal);
                                messageDetails.XML_MsgResponse.append(XMLchars.Quote);
                                messageDetails.XML_MsgResponse.append(XMLchars.Quote);
                            }
                        }
                    }
                    else {
                        String messageException = "1й проход, достаём атрибуты элемента: XML_Current_Tags [" + Current_Elm_Key + "] messageDetails.Message.get[" + i + "] вернуло NULL! Tag_Id: <" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Id + "> Tag_Value=" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Value +
                                "; Tag_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Num +
                                "; Tag_Par_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Par_Num;
                        MessegeReceive_Log.error(messageException);
                        throw new NullPointerException( messageException);
                    }
                }
                messageDetails.XML_MsgResponse.append(XMLchars.CloseTag); // + ">" );
                if ( messageDetailVO.Tag_Value != null )
                    messageDetails.XML_MsgResponse.append(messageDetailVO.getTag_Value());

                Iterator<Integer> ChildVO_ElementIterator = ChildVO_ArrayList.iterator();
                // 2й проход, достаём дочерние элементы
                while (ChildVO_ElementIterator.hasNext()) {
                    Integer i = ChildVO_ElementIterator.next();
                    messageChildVO = messageDetails.Message.get(i);
                    if (messageChildVO != null) {
                        if ((messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент - проверка!!!
                                (messageChildVO.Tag_Num != 0))  // И это элемент, который может быть потомком!
                        {  // вызываем рекурсию
                            XML_Current_Tags(messageDetails, i);
                        }
                    }
                    else {
                        String messageException = "2й проход, достаём дочерние элементы: XML_Current_Tags [" + Current_Elm_Key + "] messageDetails.Message.get[" + i + "] вернуло NULL! Tag_Id: <" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Id + "> Tag_Value=" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Value +
                                "; Tag_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Num +
                                "; Tag_Par_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Par_Num;
                        MessegeReceive_Log.error(messageException);
                        throw new NullPointerException( messageException);
                    }
                }

            } else { // вложенных элементов нет, чисто значение
                messageDetails.XML_MsgResponse.append(XMLchars.CloseTag); // + ">" );
                if ( messageDetailVO.Tag_Value != null )
                    messageDetails.XML_MsgResponse.append(messageDetailVO.getTag_Value());
            }

            messageDetails.XML_MsgResponse.append(XMLchars.OpenTag ); // <\Tag_Id>
            messageDetails.XML_MsgResponse.append( XMLchars.EndTag );
            messageDetails.XML_MsgResponse.append( messageDetailVO.Tag_Id );
            messageDetails.XML_MsgResponse.append( XMLchars.CloseTag);
            return 1; //XML_Tag;
        } else {
            // !было: StringBuilder XML_Tag = new StringBuilder(XMLchars.Space);
            return 0; //XML_Tag;
        }
    }


    public static boolean isMessageQueue_Direction_EXEIN(TheadDataAccess theadDataAccess, long Queue_Id, MessageQueueVO messageQueueVO, boolean isDebugged, Logger MessegeReceive_Log) {
          String Queue_Direction=null;
            try {
                theadDataAccess.stmtMsgQueue.setLong(1,  Queue_Id );
                ResultSet rs = theadDataAccess.stmtMsgQueue.executeQuery();
                while (rs.next()) {
                    Queue_Direction = rs.getString("Queue_Direction");
                    messageQueueVO.setMsg_Reason( rs.getString("Msg_Reason") );
                    messageQueueVO.setMsg_Result( rs.getString("Msg_Result") );
                    if ( isDebugged )
                    MessegeReceive_Log.info( "["+ Queue_Id +"] isMessageQueue_Direction_EXEIN: Direction=" + rs.getString("Queue_Direction") +
                            " Msg_status:[" + rs.getString("Msg_status") + "] Msg_Reason=" + rs.getString("Msg_Reason") +
                            "] Msg_Result=" + rs.getString("Msg_Result") + " Outqueue_id=" + rs.getLong("OutQueue_id"));
                    // Очистили Message от всего, что там было

                }
                rs.close();
            } catch (Exception e) {
                MessegeReceive_Log.error(e.getMessage());
                System.err.println("["+ Queue_Id +"] select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage());
                //e.printStackTrace();
                MessegeReceive_Log.error( "["+ Queue_Id +"] select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
                return false;
            }
            if ( Queue_Direction != null)
            return Queue_Direction.equals(XMLchars.DirectEXEIN);
            else
                return false;
        }

    public static boolean isLink_Queue_Finish(TheadDataAccess theadDataAccess, long Queue_Id, boolean isDebugged, Logger MessegeReceive_Log) {
        String Queue_Direction=null;
        try {
            theadDataAccess.stmtMsgQueue.setLong(1,  Queue_Id );
            ResultSet rs = theadDataAccess.stmtMsgQueue.executeQuery();
            while (rs.next()) {
                Queue_Direction = rs.getString("Queue_Direction");
                if ( isDebugged )
                MessegeReceive_Log.info( "["+ Queue_Id +"] isLink_Queue_Finish:" + rs.getString("Queue_Direction") +
                        " Msg_status:[ " + rs.getString("Msg_status") + "] Msg_Reason=" + rs.getString("Msg_Reason") +
                        "] Msg_Result=" + rs.getString("Msg_Result"));
                // Очистили Message от всего, что там было

            }
            rs.close();
        } catch (Exception e) {
            MessegeReceive_Log.error(e.getMessage());
            System.err.println("["+ Queue_Id +"] isLink_Queue_Finish(): select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
            e.printStackTrace();
            MessegeReceive_Log.error( "["+ Queue_Id +"] select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
            return false;
        }
        if ( Queue_Direction != null)
            return (  Queue_Direction.equals(XMLchars.DirectERROUT) || Queue_Direction.equals(XMLchars.DirectATTNOUT) || Queue_Direction.equals(XMLchars.DirectDELOUT) );
        else
            return false;
    }

    public static String get_Link_Queue_Finish(TheadDataAccess theadDataAccess, long Queue_Id, StringBuilder pXML_MsgConfirmation, boolean isDebugged, Logger MessegeReceive_Log) {
        String Queue_Direction =null;
        pXML_MsgConfirmation.setLength(0); pXML_MsgConfirmation.trimToSize();
        try {
            theadDataAccess.stmtMsgQueue.setLong(1,  Queue_Id );
            ResultSet rs = theadDataAccess.stmtMsgQueue.executeQuery();
            while (rs.next()) {
                Queue_Direction = rs.getString("Queue_Direction");
                pXML_MsgConfirmation.setLength(0); pXML_MsgConfirmation.trimToSize();
                pXML_MsgConfirmation.append( rs.getString("Msg_Reason"));
                if ( isDebugged )
                    MessegeReceive_Log.info( "["+ Queue_Id +"] get_Link_Queue_Finish:" + rs.getString("Queue_Direction") +
                            " Msg_status:[ " + rs.getString("Msg_status") + "] Msg_Reason=" + rs.getString("Msg_Reason") +
                            "] Msg_Result=" + rs.getString("Msg_Result"));
                // Очистили Message от всего, что там было

            }
            rs.close();
        } catch (Exception e) {
            //MessegeReceive_Log.error(e.getMessage());
            System.err.println("["+ Queue_Id +"] get_Link_Queue_Finish(): select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
            e.printStackTrace();
            MessegeReceive_Log.error( "["+ Queue_Id +"] get_Link_Queue_Finish(): select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
            return null;
        }
        if ( Queue_Direction != null)
            return Queue_Direction;
        else
            return null;
    }


    public static int get_SelectLink_msg_InfostreamId(TheadDataAccess theadDataAccess, long Queue_Id,  boolean isDebugged, Logger MessegeReceive_Log)
    {
        int msg_infostreamid = -1;
        try {
            theadDataAccess.stmtMsgQueue.setLong(1,  Queue_Id );
            ResultSet rs = theadDataAccess.stmtMsgQueue.executeQuery();
            while (rs.next()) {
                msg_infostreamid = rs.getInt("msg_infostreamid");
                if ( isDebugged )
                MessegeReceive_Log.info( "["+ Queue_Id +"] get_SelectLink_msg_InfostreamId:" + rs.getString("Queue_Direction") +
                        " Msg_status:[ " + rs.getString("Msg_status") + "] Msg_Reason=" + rs.getString("Msg_Reason") +
                        "] msg_infostreamid =" + rs.getInt("msg_infostreamid"));
                // Очистили Message от всего, что там было

            }
            rs.close();
        } catch (Exception e) {
            MessegeReceive_Log.error(e.getMessage());
            System.err.println("Queue_Id=[" + Queue_Id + "] get_SelectLink_msg_InfostreamId() select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault " + sStackTrace.strInterruptedException(e));
            MessegeReceive_Log.error( "["+ Queue_Id +"] get_SelectLink_msg_InfostreamId() select Queue_Direction from MESSAGE_QUEUE q Where  Q.queue_id = ? fault: " + e.getMessage() + " что то пошло совсем не так...");
            return -2;
        }

        return msg_infostreamid;
    }

    public static Long get_SelectLink_Queue_Id(TheadDataAccess theadDataAccess, long Queue_Id, MessageQueueVO messageQueueVO, boolean isDebugged, Logger MessegeReceive_Log)
    {
        Long Link_Queue_Id=null;
        try {
            theadDataAccess.stmt_SELECT_Link_Queue_Id.setLong(1,  Queue_Id );
            ResultSet rs = theadDataAccess.stmt_SELECT_Link_Queue_Id.executeQuery();
            while (rs.next()) {
                Link_Queue_Id = rs.getLong("Link_Queue_Id");
                messageQueueVO.setMsg_Reason( rs.getString("Msg_Reason") );
                if ( isDebugged )
                MessegeReceive_Log.info( "["+ Queue_Id +"] get_SelectLink_Queue_Id:" + rs.getLong("Link_Queue_Id"));
                if ( Link_Queue_Id == 0L ) Link_Queue_Id = null;
            }
            rs.close();
        } catch (Exception e) {
            System.err.println("Queue_Id=[" + Queue_Id + "] :" + theadDataAccess.SELECT_Link_Queue_Id + "fault " + sStackTrace.strInterruptedException(e));
            MessegeReceive_Log.error( "["+ Queue_Id +"] " + theadDataAccess.SELECT_Link_Queue_Id + "fault " + e.getMessage() + " что то пошло совсем не так...");
            return Link_Queue_Id;
        }
            return Link_Queue_Id;
    }

    public static int ReadConfirmation(@NotNull TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeReceive_Log) {

            messageDetails.Confirmation.clear();
            messageDetails.ConfirmationRowNum = 0;
            messageDetails.Confirmation_Tag_Num = 0;
            messageDetails.XML_MsgConfirmation.setLength(0);
            messageDetails.XML_MsgConfirmation.trimToSize();
            int Tag_Num=-1;

            try { // получаем Confirmation Tag_Num из select Tag_Num from  Message_QueueDet  WHERE QUEUE_ID = ?Queue_Id and Tag_Par_Num = 0 and tag_Id ='Confirmation'
                theadDataAccess.stmtMsgQueueConfirmationTag.setLong(1, Queue_Id);
                ResultSet rs = theadDataAccess.stmtMsgQueueConfirmationTag.executeQuery();
                while (rs.next()) {
                    Tag_Num= rs.getInt("Tag_Num");
                }
                rs.close();
            } catch (SQLException e) {
                MessegeReceive_Log.error("Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
                System.err.println("Queue_Id=[" + Queue_Id + "] :" + e.getMessage() );
                e.printStackTrace();
                return messageDetails.ConfirmationRowNum;
            }
            if ( Tag_Num < 1 ) {
                MessegeReceive_Log.error("Queue_Id=[" + Queue_Id + "] ReadConfirmation: tag 'Confirmation' не найден в MESSAGE_QUEUEDET" );
                return -1;
            }
            try {
                theadDataAccess.stmtMsgQueueConfirmationDet.setLong(1, Queue_Id);
                theadDataAccess.stmtMsgQueueConfirmationDet.setInt(2, Tag_Num);
                theadDataAccess.stmtMsgQueueConfirmationDet.setLong(3, Queue_Id);
                theadDataAccess.stmtMsgQueueConfirmationDet.setInt(4, Tag_Num);
                ResultSet rs = theadDataAccess.stmtMsgQueueConfirmationDet.executeQuery();
                String rTag_Value=null;
                while (rs.next()) {
                    MessageDetailVO messageDetailVO = new MessageDetailVO();
                    rTag_Value = StringEscapeUtils.escapeXml10(rs.getString("Tag_Value") );
//                    MessegeReceive_Log.warn("_ReadConfirmation messageChildVO.Tag_Par_Num=" + rs.getInt("Tag_Par_Num") +
//                            ", messageChildVO.Tag_Num=" + rs.getInt("Tag_Num") +
//                            ", messageChildVO.Tag_Id=" + rs.getString("Tag_Id") +
//                            ", messageChildVO.Tag_Value=" + rTag_Value
//                    );

                    if ( rTag_Value == null )
                    messageDetailVO.setMessageQueue(
                            rs.getString("Tag_Id"),
                            null,
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                    else
                        messageDetailVO.setMessageQueue(
                                rs.getString("Tag_Id"),
                                StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                                //StringEscapeUtils.escapeXml10(rTag_Value.replaceAll(XMLchars.XML10pattern,"")),
                                // StringEscapeUtils.escapeXml10(rTag_Value).replaceAll(XMLchars.XML10pattern,""),
                                rs.getInt("Tag_Num"),
                                rs.getInt("Tag_Par_Num")
                        );
                    messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
                    messageDetails.ConfirmationRowNum += 1;
                    // MessegeReceive_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
                }
                rs.close();
            } catch (SQLException e) {
                MessegeReceive_Log.error("Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
                e.printStackTrace();
                return -2;
            }
//                if (  messageDetails.MessageTemplate4Perform.getIsDebugged() ) {
//                    for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
//                        MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);
//                        MessegeReceive_Log.warn("_ReadConfirmation done messageChildVO.Tag_Par_Num=" + messageChildVO.Tag_Par_Num +
//                                ", messageChildVO.Tag_Num=" + messageChildVO.Tag_Num +
//                                ", messageChildVO.Tag_Id=" + messageChildVO.Tag_Id +
//                                ", messageChildVO.Tag_Value=" + messageChildVO.Tag_Value
//                        );
//                    }
//                }
            if ( messageDetails.ConfirmationRowNum > 0 )


            XML_CurrentConfirmation_Tags(messageDetails, 0, MessegeReceive_Log);
            if (  messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeReceive_Log.info("["+ Queue_Id +"] MsgConfirmation: " +  messageDetails.XML_MsgConfirmation.toString());
            return messageDetails.ConfirmationRowNum;
        }

    public static String stripNonValidXMLCharacters(String in) {
        if (in == null || in.isEmpty() || ("".equals(in))) return ""; // vacancy test.
        StringBuilder out = new StringBuilder( in.length() ); // Used to hold the output.
        char current; // Used to reference the current character.

        for (int i = 0; i < in.length(); i++) {
            current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
            if ((current == 0x9) ||
                    (current == 0xA) ||
                    (current == 0xD) ||
                    ((current >= 0x20) && (current <= 0xD7FF)) ||
                    ((current >= 0xE000) && (current <= 0xFFFD)) ||
                    ((current >= 0x10000) && (current <= 0x10FFFF)))
                out.append(current);
        }
        return out.toString();
    }

    // @messageDetails.XML_Confirmation формируется из messageDetails.Confirmation

    public static int XML_CurrentConfirmation_Tags(MessageDetails messageDetails, int Current_Elm_Key, Logger MessegeReceive_Log) {
        MessageDetailVO messageDetailVO = messageDetails.Confirmation.get(Current_Elm_Key);
//        boolean is_Local_Debug= true;
        if ( messageDetailVO.Tag_Num != 0 ) {

            // !было:  StringBuilder XML_Tag = new StringBuilder( XMLchars.OpenTag + messageDetailVO.Tag_Id );
            // стало:
            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag).append(messageDetailVO.Tag_Id);
            // XML_Tag.append ( "<" + messageDetailVO.Tag_Id + ">" );
            // цикл по формированию параметров-аьтрибутов элемента
            for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);
//                MessegeReceive_Log.warn("_CurrentConfirmation_Tags messageChildVO.Tag_Par_Num=" +  messageChildVO.Tag_Par_Num +
//                                ", messageChildVO.Tag_Num=" + messageChildVO.Tag_Num +
//                                ", messageChildVO.Tag_Id=" + messageChildVO.Tag_Id +
//                                ", messageChildVO.Tag_Value=" + messageChildVO.Tag_Value
//                );
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num == 0) )  // это атрибут элемента, у которого нет потомков
                {
                    if ( messageChildVO.Tag_Value != null )
                        // !было:XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_MsgConfirmation.append(XMLchars.Space).append(messageChildVO.Tag_Id).
                                append(XMLchars.Equal).append(XMLchars.Quote).append(messageChildVO.Tag_Value).append(XMLchars.Quote);
        // по
        //                messageDetails.XML_MsgConfirmation.append(XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote);
                    else
                        // !было: XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_MsgConfirmation.append(XMLchars.Space)
                                                          .append( messageChildVO.Tag_Id)
                                                          .append( XMLchars.Equal)
                                                          .append( XMLchars.Quote).append( "noName").append(XMLchars.Quote);
                }
            }
            // !было: XML_Tag.append( XMLchars.CloseTag);
            // стало:
            messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);

            if ( messageDetailVO.Tag_Value != null )
                // !было: XML_Tag.append( messageDetailVO.getTag_Value() );
                // стало:
                messageDetails.XML_MsgConfirmation.append(messageDetailVO.getTag_Value());

            for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);

                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num != 0) )  // И это элемент, который может быть потомком!
                {
                    // !было: XML_Tag.append ( XML_Current_Tags( messageDetails, i) );
                    // стало:
                    XML_CurrentConfirmation_Tags(messageDetails, i, MessegeReceive_Log );
                }
            }

            // !было: XML_Tag.append( XMLchars.OpenTag + XMLchars.EndTag + messageDetailVO.Tag_Id + XMLchars.CloseTag);
            // стало:
            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag).append(XMLchars.EndTag).append( messageDetailVO.Tag_Id ).append(XMLchars.CloseTag);
            return 1; //XML_Tag;
        } else {
            // !было: return 0;
            // Теряются отрибуты по считывании Confirmation
            if ( messageDetailVO.Tag_Value != null ) {   // !было:XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote );
                // стало:
                messageDetails.XML_MsgConfirmation.append(XMLchars.Space).append( messageDetailVO.Tag_Id).append( XMLchars.Equal)
                                                  .append( XMLchars.Quote).append( messageDetailVO.Tag_Value).append( XMLchars.Quote);
                return 1; //XML_Tag;
            }
            else
            return 0; //XML_Tag;
        }
    }


    public static int SaveMessage4Input(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, MessageQueueVO messageQueueVO , Logger MessegeReceive_Log) {
        int nn = 0;
        // Надо переносить переинициализацию messageDetails.Message после того, как распарселили новый XML
        // messageDetails.Message.clear();
        // messageDetails.MessageRowNum = 0;
        // а тут закоментарили !
        //String parsedMessage4SEND = messageDetails.XML_MsgSEND;
        //AppThead_log.info( parsedConfig );

        // if ( parsedMessage4SEND.length() == 0 ) return -3;
        if ( messageDetails.Request_Method == null ) return -2;
        //MessegeReceive_Log.warn("SaveMessage4Input begin");
        try {
            // SAXBuilder documentBuilder = new SAXBuilder();
            //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            //InputStream parsedMessageStream = new ByteArrayInputStream(parsedMessage4SEND.getBytes(StandardCharsets.UTF_8));
            //Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);
            // Document document = messageDetails.Input_Clear_XMLDocument;

            Element RootElement = messageDetails.Request_Method; // document.getRootElement();
            // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
            // Надо переносить пепеинициализацию messageDetails.Message после того, как распарселили новый XML
            //int Tag_Par_Num = 0;
            messageDetails.Message_Tag_Num = 0;
            messageDetails.Message.clear();
            messageDetails.MessageRowNum = 0;

            // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
            SplitMessage(messageDetails, RootElement, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                    MessegeReceive_Log);

        } catch (NullPointerException  ex) { // | IOException
            ex.printStackTrace(System.err);
            MessegeReceive_Log.error("[" + Queue_Id + "] SaveMessage4Input fault:" + ex.getMessage());
            MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, messageDetails,  theadDataAccess,
                    "SaveMessage4Input.SAXBuilder fault:"  + ex.getMessage() + " " + messageDetails.XML_MsgClear.toString()  ,
                    null ,  MessegeReceive_Log);
            return -22; // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
        }
        //MessegeReceive_Log.warn("SaveMessage4Input SplitMessage complete");
        // Замещаем полученным массивом messageDetails.Message строки в БД
        nn = MessageUtils.InsertMessageDetail(theadDataAccess, Queue_Id, messageDetails, MessegeReceive_Log);
        //MessegeReceive_Log.warn("SaveMessage4Input finish");
        return nn;
    }

    public static int SplitMessage(MessageDetails messageDetails, Element EntryElement, int tag_Par_Num,
                            Logger MessegeReceive_Log) {
        // Tag_Par_Num- №№ Тага, к которому прилепляем всё от EntryElement,ссылка на Tag_Num родителя
        // Tag_Num - сквозной!!! нумератор записей

        int nn = 0;
        //MessegeReceive_Log.info("Split[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + EntryElement.getName() + ">");

        // if ( EntryElement.isRootElement() ) ntgthm
        if ( messageDetails.Message_Tag_Num == 0){
            // Tag_Num += 1;
            String ElementPrefix = EntryElement.getNamespacePrefix();
            String ElementEntry;
            if ( ElementPrefix.length() > 0 ) {
                ElementEntry = ElementPrefix + ":" + EntryElement.getName();
            } else
                ElementEntry = EntryElement.getName();


            //MessegeReceive_Log.info("SplitMessageж Tag_Par_Num[0][1]: <" + ElementEntry + ">");
            MessageDetailVO messageDetailVO = new MessageDetailVO();
            messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                    "", // Tag_Value
                    1,
                    0
            );
            messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
            messageDetails.MessageRowNum += 1;

            List<Namespace> ElementNamespaces = EntryElement.getNamespacesIntroduced();
            for (int j = 0; j < ElementNamespaces.size(); j++) {
                // Namespace не увеличивает Tag_Num ( сквозной нумератор записей )
                // в БД имеет Tag_Num= 0, ссылается на элемент.
                Namespace Namespace = ElementNamespaces.get(j);

                MessegeReceive_Log.info("Tag_Par_Num[1][0]: " + XMLchars.XMLns + Namespace.getPrefix() + "=" + Namespace.getURI());
                MessageDetailVO NSmessageDetailVO = new MessageDetailVO();
                NSmessageDetailVO.setMessageQueue(XMLchars.XMLns + Namespace.getPrefix(), // "Tag_Id"
                        Namespace.getURI(), // Tag_Value
                        0,
                        1
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, NSmessageDetailVO);
                messageDetails.MessageRowNum += 1;

            }
            // после заполнения данных для корневого элемента, для всех его детей нужен  Tag_Par_Num== 1 !
            tag_Par_Num = 1;
            messageDetails.Message_Tag_Num += 1;
        }

        String ElementPrefix;
        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);

            ElementPrefix = XMLelement.getNamespacePrefix();
            String ElementEntry;
            if ( ElementPrefix.length() > 0 ) {
                ElementEntry = ElementPrefix + ":" + XMLelement.getName();
            } else
                ElementEntry = XMLelement.getName();

            String ElementContent = XMLelement.getText();
            MessageDetailVO messageDetailVO = new MessageDetailVO();

            messageDetails.Message_Tag_Num += 1;

            if ( ElementContent.length() > 0 ) {
                //MessegeReceive_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">=" + ElementContent);
                messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                        ElementContent, // Tag_Value
                        messageDetails.Message_Tag_Num,
                        tag_Par_Num
                );
            } else {
                //MessegeReceive_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">");

                messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                        "", // Tag_Value
                        messageDetails.Message_Tag_Num,
                        tag_Par_Num // Tag_Num += 1; будет сделано в Tag_Par_Num
                );
            }
            messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
            messageDetails.MessageRowNum += 1;
            String AttributePrefix;
            String AttributeEntry;

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                Attribute XMLattribute = ElementAttributes.get(j);
                MessageDetailVO ATTRmessageDetailVO = new MessageDetailVO();
                AttributePrefix = XMLattribute.getNamespacePrefix();
                if ( AttributePrefix.length() > 0 ) {
                    AttributeEntry = AttributePrefix + ":" + XMLattribute.getName();
                } else
                    AttributeEntry = XMLattribute.getName();
                String AttributeValue = XMLattribute.getValue();
                // Attribute не увеличивает Tag_Num ( сквозной нумератор записей )
                // в БД имеет Tag_Num= 0, ссылается на элемент.
               // MessegeReceive_Log.info("Tag_Par_Num[" + messageDetails.Message_Tag_Num + "][" + 0 + "]: \"" + AttributeEntry + "\"=" + AttributeValue);
                ATTRmessageDetailVO.setMessageQueue(AttributeEntry, // "Tag_Id"
                        AttributeValue, // Tag_Value
                        0,
                        messageDetails.Message_Tag_Num
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, ATTRmessageDetailVO);
                messageDetails.MessageRowNum += 1;
            }
            // Tag_Par_Num += 1;  /// ??????????????????????????????????? Явно не то.
            // int tag_Par_Num_4_Child = Tag_Num.intValue();
            SplitMessage(messageDetails, XMLelement,
                    messageDetails.Message_Tag_Num, // Tag_Par_Num  для рекурсии
                    // Tag_Num, // Tag_Num += 1; будет сделано в цикле по дочерним элементам перед добавлением
                    MessegeReceive_Log);

        }
        return nn;
    }

    public static int InsertMessageDetail(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, Logger MessegeReceive_Log) {
        int nn = 0;
/*
        try {
            theadDataAccess.stmt_DELETE_Message_Details.setLong(1, Queue_Id);
            theadDataAccess.stmt_DELETE_Message_Details.executeUpdate();
        } catch (SQLException e) {
            MessegeReceive_Log.error("DELETE(" + theadDataAccess.DELETE_Message_Details + "[" + Queue_Id + "]" + ") fault: " + e.getMessage());
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return -1;
        }
*/
        MessageDetailVO messageDetailVO=null;
        try {

            for (int i = 0; i < messageDetails.Message.size(); i++) {
                messageDetailVO = messageDetails.Message.get(i);
                theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(2, messageDetailVO.Tag_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(3, StringEscapeUtils.unescapeXml(messageDetailVO.Tag_Value));
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, messageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, messageDetailVO.Tag_Par_Num);
                // Insert data in Oracle with Java … Batched mode
                // theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();
                theadDataAccess.stmt_INSERT_Message_Details.addBatch();
        /*MessegeReceive_Log.info( i + ">" + theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]" +
                "\n Tag_Id=" + MessageDetailVO.Tag_Id +
                "\n Tag_Value=" + MessageDetailVO.Tag_Value +
                "\n Tag_Num=" + MessageDetailVO.Tag_Num +
                "\n Tag_Par_Num=" + MessageDetailVO.Tag_Par_Num +
                " done");
                */
                nn = i;
            }
            // Insert data in Oracle with Java … Batched mode
            theadDataAccess.stmt_INSERT_Message_Details.executeBatch();

        } catch (SQLException e) {
            MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            System.err.println(":Queue_Id=[" + Queue_Id + "] :" + theadDataAccess.INSERT_Message_Details );
            System.err.println(StringEscapeUtils.unescapeXml(messageDetailVO.Tag_Value));
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                System.err.println(":Queue_Id=[" + Queue_Id + "] :" + "Hermes_Connection.rollback() fault: " + exp.getMessage());
                MessegeReceive_Log.error("Hermes_Connection.rollback() fault: " + exp.getMessage());
            }
            return -2;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("Hermes_Connection.commit() fault: " + exp.getMessage());
            return -3;
        }

        // MessegeReceive_Log.info(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :  INSERT new Message Details, " + nn + " rows done");
        return nn;
    }


    public static int SplitConfirmation(MessageDetails messageDetails, Element EntryElement, int tag_Par_Num,
                                   Logger MessegeReceive_Log) {
        // Tag_Par_Num- №№ Тага, к которому прилепляем всё от EntryElement,ссылка на Tag_Num родителя
        // Tag_Num - сквозной!!! нумератор записей

        int nn = 0;
        //MessegeReceive_Log.info("SplitConfirmation[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + EntryElement.getName() + ">");

        if ( EntryElement.getName().equals( XMLchars.TagConfirmation )) {
            messageDetails.ConfirmationRowNum = 0;
            String  ElementEntry = EntryElement.getName();
            //MessegeReceive_Log.info("Tag_Par_Num[0]["+ messageDetails.Message_Tag_Num +"]: <" + ElementEntry + ">");
            MessageDetailVO messageDetailVO = new MessageDetailVO();
            messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                    "", // Tag_Value
                    messageDetails.Message_Tag_Num,
                    0
            );
            messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
            messageDetails.ConfirmationRowNum += 1;
            // после заполнения данных для корневого элемента, для всех его детей нужен  Tag_Par_Num== messageDetails.Message_Tag_Num,
            // который был установлен ПЕРЕД кукурсивным SplitConfirmation!
            tag_Par_Num = messageDetails.Message_Tag_Num;

        }

        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);

            String ElementEntry = XMLelement.getName();
            if (( XMLelement.getParentElement().getName().equals( XMLchars.TagConfirmation ) )
                &&
                    (XMLelement.getName().equals(XMLchars.TagNext)) )
            {
                ; // пропускаем /Confirmation/Next
            }
            else {
                String ElementContent = XMLelement.getText();
                MessageDetailVO messageDetailVO = new MessageDetailVO();

                messageDetails.Message_Tag_Num += 1;

                if ( ElementContent.length() > 0 ) {
                    //MessegeReceive_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">=" + ElementContent);
                    messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                            ElementContent, // Tag_Value
                            messageDetails.Message_Tag_Num,
                            tag_Par_Num
                    );
                } else {
                    //MessegeReceive_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">");

                    messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                            "", // Tag_Value
                            messageDetails.Message_Tag_Num,
                            tag_Par_Num // Tag_Num += 1; будет сделано в Tag_Par_Num
                    );
                }
                messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
                messageDetails.ConfirmationRowNum += 1;

                List<Attribute> ElementAttributes = XMLelement.getAttributes();
                for (int j = 0; j < ElementAttributes.size(); j++) {
                    Attribute XMLattribute = ElementAttributes.get(j);
                    MessageDetailVO ATTRmessageDetailVO = new MessageDetailVO();
                    String AttributeEntry = XMLattribute.getName();
                    String AttributeValue = XMLattribute.getValue();
                    // Attribute не увеличивает Tag_Num ( сквозной нумератор записей )
                    // в БД имеет Tag_Num= 0, ссылается на элемент.
                    //MessegeReceive_Log.info("Tag_Par_Num[" + messageDetails.Message_Tag_Num + "][" + 0 + "]: \"" + AttributeEntry + "\"=" + AttributeValue);
                    ATTRmessageDetailVO.setMessageQueue(AttributeEntry, // "Tag_Id"
                            AttributeValue, // Tag_Value
                            0,
                            messageDetails.Message_Tag_Num
                    );
                    messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, ATTRmessageDetailVO);
                    messageDetails.ConfirmationRowNum += 1;
                }

                // int tag_Par_Num_4_Child = Tag_Num.intValue();
                SplitConfirmation(messageDetails, XMLelement,
                        messageDetails.Message_Tag_Num, // Tag_Par_Num  для рекурсии
                        // Tag_Num, // Tag_Num += 1; будет сделано в цикле по дочерним элементам перед добавлением
                        MessegeReceive_Log);
            }

        }
        return nn;
    }


    public static int ReplaceConfirmation(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, Logger MessegeReceive_Log) {
        int nn = 0;

        nn = theadDataAccess.doDELETE_Message_Confirmation( Queue_Id, MessegeReceive_Log);
        if ( nn < 0 )
            return -1;
        int iNumberRecordInConfirmation=0;

        try {
            for ( iNumberRecordInConfirmation = 0; iNumberRecordInConfirmation < messageDetails.Confirmation.size(); iNumberRecordInConfirmation++) {
                MessageDetailVO MessageDetailVO = messageDetails.Confirmation.get( iNumberRecordInConfirmation );
                theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(2, MessageDetailVO.Tag_Id);
                // StringEscapeUtils.unescapeXml(MessageDetailVO.Tag_Value);
                //theadDataAccess.stmt_INSERT_Message_Details.setString(3, MessageDetailVO.Tag_Value);
                theadDataAccess.stmt_INSERT_Message_Details.setString(3, StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                // MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+  MessageDetailVO.Tag_Id +"] :" + StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, MessageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, MessageDetailVO.Tag_Par_Num);
                theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();

                nn = iNumberRecordInConfirmation;
            }
        } catch ( Exception e) {
            MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+ iNumberRecordInConfirmation +"] :" + sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append( "ReplaceConfirmation [").append( iNumberRecordInConfirmation).append("] ").append( sStackTrace.strInterruptedException(e) );
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return -2;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            return -3;
        }

        MessegeReceive_Log.info(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + nn + " done");
        return nn;
    }

}
