package net.plumbing.msgbus.controller;

//import com.google.common.collect.ImmutableMap;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplate4Perform;
import net.plumbing.msgbus.threads.ExtSystemDataConnection;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.plumbing.msgbus.threads.utils.*;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.mq.PerformTextMessageJMSQueue;
import net.plumbing.msgbus.mq.StoreMQpooledConnectionFactory;

import java.net.Authenticator;
import java.net.http.HttpClient;
import java.sql.SQLException;
import java.time.Duration;
//import java.util.HashMap;
//import java.util.concurrent.TimeUnit;
//import java.io.IOException;
import javax.jms.JMSException;
import javax.net.ssl.SSLContext;
import javax.xml.transform.TransformerException;

import net.plumbing.msgbus.threads.utils.MessageHttpSend;


public class PerfotmInputMessages {

    //private DefaultHttpClient client=null;
    //private CloseableHttpClient httpClient=null;
    // private ThreadSafeClientConnManager ExternalConnectionManager;
    // private String ConvXMLuseXSLTerr = "";
    //  org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

  //  public void setExternalConnectionManager( ThreadSafeClientConnManager externalConnectionManager ) {
  //      this.ExternalConnectionManager = externalConnectionManager;
  //  }

    // private Security endpointProperties;

    public  long performMessage(MessageDetails Message, MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, xlstErrorListener XSLTErrorListener, StringBuilder ConvXMLuseXSLTerr, Logger MessegeReceive_Log) {
        // 1. Получаем шаблон обработки для MessageQueueVO
        String SubSys_Cod = messageQueueVO.getSubSys_Cod();
        int MsgDirection_Id = messageQueueVO.getMsgDirection_Id();
        int Operation_Id = messageQueueVO.getOperation_Id();
        Long Queue_Id = messageQueueVO.getQueue_Id();
        Long Link_Queue_Id = null;
        String Queue_Direction = messageQueueVO.getQueue_Direction();

        int Function_Result = 0;


        //MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "] ищем Шаблон под оперрацию (" + Operation_Id + "), с учетом системы приёмника MsgDirection_Id=" + MsgDirection_Id + ", SubSys_Cod =" + SubSys_Cod);

        // ищем Шаблон под оперрацию, с учетом системы приёмника MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        int Template_Id = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(Operation_Id, MsgDirection_Id, SubSys_Cod, MessegeReceive_Log);
        MessegeReceive_Log.info("[" + Queue_Id + "] `" + Queue_Direction + "` look4MessageTemplateVO_2_Perform Шаблон под оперрацию (" + Operation_Id + "), с учетом системы приёмника MsgDirection_Id=" + MsgDirection_Id + ", SubSys_Cod =" + SubSys_Cod + " : вернул Template_Id="  + Template_Id);

        //MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "]  Шаблон под оперрацию =" + Template_Id);

        if ( Template_Id < 0 ) {
            String pMessage_Reason = "Не нашли шаблон обработки сообщения тип=" + Operation_Id + ", для комбинации: Идентификатор системы[" + MsgDirection_Id + "] Код подсистемы[" + SubSys_Cod + "]";
            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, pMessage_Reason,
                    3101, MessegeReceive_Log);
            Message.MsgReason.append("[" + Queue_Id + "] "); Message.MsgReason.append( pMessage_Reason);
            return -15L;
        }

        int messageTypeVO_Key = MessageRepositoryHelper.look4MessageTypeVO_2_Perform(Operation_Id, MessegeReceive_Log);
        if ( messageTypeVO_Key < 0 ) {
            MessegeReceive_Log.error("[" + Queue_Id + "] MessageRepositoryHelper.look4MessageTypeVO_2_Perform: Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]");
            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]", 3102, MessegeReceive_Log);
            return -17L;
        }

        Message.MessageTemplate4Perform = new MessageTemplate4Perform(MessageTemplate.AllMessageTemplate.get(Template_Id),
                Queue_Id,
                MessegeReceive_Log
        );
        if ( Message.MessageTemplate4Perform.getIsDebugged() )
            MessegeReceive_Log.info("[" + Queue_Id + "] MessageTemplate4Perform[" + Message.MessageTemplate4Perform.printMessageTemplate4Perform() );
        boolean is_NoConfirmation = // Признак на типе сообщения, что Confirmation формируется в памяти, messageDetails.XML_MsgConfirmation
                MessageRepositoryHelper.isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation(messageQueueVO.getOperation_Id(), MessegeReceive_Log);

        switch (Queue_Direction){
            case XMLchars.DirectNEWIN:

                if ( Message.MessageTemplate4Perform.getMessageXSD() != null )
                { boolean is_Message_OUT_Valid =
                    XMLutils.TestXMLByXSD( Queue_Id,  Message.XML_Request_Method.toString(), Message.MessageTemplate4Perform.getMessageXSD(), Message.MsgReason, MessegeReceive_Log );
                    if ( ! is_Message_OUT_Valid ) {
                        MessegeReceive_Log.error("[" + Queue_Id + "] validateXMLSchema: message (" + Message.XML_Request_Method.toString() + ") is not valid for XSD " + Message.MessageTemplate4Perform.getMessageXSD());

                        theadDataAccess.doUPDATE_MessageQueue_Temp2ErrIN(Queue_Id,
                                messageQueueVO.getOperation_Id(),
                                messageQueueVO.getMsgDirection_Id(), messageQueueVO.getSubSys_Cod(),
                                messageQueueVO.getMsg_Type(), messageQueueVO.getMsg_Type_own(),
                                Message.MsgReason.toString(),
                                0L,
                                MessegeReceive_Log);

                        MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO,   Message,  theadDataAccess,
                                "validateXMLSchema: message " + Message.MsgReason.toString() + " XSD" + Message.MessageTemplate4Perform.getMessageXSD() ,
                                null ,  MessegeReceive_Log);
                        // Считаем, что виноват клиент
                        return 13L;
                    }
                }


                // для запросов, на интерфейсе , не предполагающих формирование блока Confirmation в БД параметры зароса берутся из памяти,
                // их сохранение имеет смысл только для отладки в режиме Debug=on
                if ( ( !is_NoConfirmation ) ||  Message.MessageTemplate4Perform.getIsDebugged() )
                // сохраняем входящее - распарсенный по-строчно <Tag><VALUE>
                Function_Result = MessageUtils.SaveMessage4Input(
                         theadDataAccess,  Queue_Id,  Message,  messageQueueVO , MessegeReceive_Log) ;
                else Function_Result =0;

                if( Function_Result < 0 ) {
                    MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                            "Не удалось сохранить содержимое сообщения в твблицу очереди:"  + " " + Message.XML_MsgClear.toString()  ,
                            null ,  MessegeReceive_Log);
                    return -19L;
                }
                try {
                    if ( MessageUtils.ProcessingIn_setIN(messageQueueVO,  theadDataAccess, MessegeReceive_Log) < 0 ) {
                        Message.MsgReason.append("Не удалось сохранить заголовок сообщения в твблицу очереди");
                        return -21L;
                    }
                }  catch (Exception e_setIN) {
                    MessegeReceive_Log.error("Не удалось сохранить заголовок сообщения в твблицу очереди: ProcessingIn_setIN fault " + e_setIN.getMessage());
                    System.err.println("ProcessingIn_setIN[" + Queue_Id + "] problen: "+ e_setIN.getMessage()); //e_setIN.printStackTrace();
                    MessegeReceive_Log.error( "Не удалось сохранить заголовок сообщения в твблицу очереди:");
                    MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                            "Не удалось сохранить заголовок сообщения в твблицу очереди:"  + e_setIN.getMessage() + " " + Message.XML_MsgClear.toString()  ,
                            e_setIN ,  MessegeReceive_Log);
                    return -23L;
                }

            case XMLchars.DirectIN:

                if ((Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) &&
                    ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) ) )
                { // 2.1) Это JDBC-обработчик, но может быть указан custom Java метод, см. PropJavaMethodName
                    //--------------------------------------------------
                    if (Message.MessageTemplate4Perform.getIsDebugged()) {
                        if  ( Message.MessageTemplate4Perform.getPropJavaMethodName() != null)
                        MessegeReceive_Log.info("[" + Queue_Id + "] getPropJavaMethodName(" +Message.MessageTemplate4Perform.getPropJavaMethodName() + ")");

                    }
                    if  ( Message.MessageTemplate4Perform.getPropJavaMethodName() != null)
                    // есть конкретный Java-обрабочик - PropJavaMethodName имя метода для кастомизации
                    { String JavaMethodName = Message.MessageTemplate4Perform.getPropJavaMethodName();
                        int resultSQL = 0;
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.info("["+ Queue_Id +"] try CustomJavaMethods for (" + JavaMethodName + ")");
                        switch (JavaMethodName) {
                            case "GetConfig_Text_Template":
                               resultSQL = CustomJavaMethods.GetConfig_Text_Template(  messageQueueVO, Message, theadDataAccess, ApplicationProperties.HrmsSchema, MessegeReceive_Log);
                               break;
                            case "GetConfig_Entry_Template":
                                resultSQL = CustomJavaMethods.GetConfig_Entrys_Template(  messageQueueVO, Message, theadDataAccess, ApplicationProperties.HrmsSchema, MessegeReceive_Log);
                                break;
                            case "GetRequest_Confirmation4Message":
                                resultSQL = CustomJavaMethods.GetRequest_Confirmation4Message(  messageQueueVO, Message, theadDataAccess, MessegeReceive_Log);
                                break;
                                //
                            case "GetRequest_Body4Message":
                                resultSQL = CustomJavaMethods.GetRequest_Body4Message(  messageQueueVO, Message, theadDataAccess, MessegeReceive_Log);
                                break;

                            case "GetResponse4MessageQueueLog":
                                resultSQL = CustomJavaMethods.GetResponse4MessageQueueLog( messageQueueVO, Message, theadDataAccess, ApplicationProperties.HrmsSchema, MessegeReceive_Log);
                                break;

                            case "GetRequest4MessageQueueLog":
                                resultSQL = CustomJavaMethods.GetRequest4MessageQueueLog( messageQueueVO, Message, theadDataAccess, ApplicationProperties.HrmsSchema, MessegeReceive_Log);
                                break;
                            case "MessageTemplates_SaveConfig" :
                                resultSQL = CustomJavaMethods.MessageTemplates_SaveConfig(  messageQueueVO, Message, theadDataAccess, ApplicationProperties.HrmsSchema, MessegeReceive_Log);
                                break;
                            default:
                                Message.MsgReason.append(" попытка вызова незарегистрированного в системе Java метода:` ");
                                Message.MsgReason.append(JavaMethodName);
                                Message.MsgReason.append("`, проконсультируйтесь с разработчиками");
                                resultSQL = -1001;
                                break;
                        }
                        if (resultSQL != 0) {

                            MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка CustomJavaMethods:" + Message.MsgReason.toString() );
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка GetConfig_Text_Template: " + Message.MsgReason.toString(), 3232,
                                    MessegeReceive_Log);
                            return -33L;
                        }
                        else
                        {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.info("["+ Queue_Id +"] Исполнение CustomJavaMethods." + JavaMethodName + " => " + resultSQL + " : Message.MsgReason (`" + Message.MsgReason.toString() +"`)" );
                        }
                        // Устанавливаеи признак завершения работы,  Message.XML_MsgResponse - результат отаботки
                        theadDataAccess.doUPDATE_MessageQueue_ExeIn2DelIN(Queue_Id, MessegeReceive_Log );
                        // завершаемся - выходим с == 0
                        return 0L;
                    }
                    //----------------------------------------------------
                    if (( Message.MessageTemplate4Perform.getEnvelopeXSLTExt() != null ) &&
                        (!Message.MessageTemplate4Perform.getEnvelopeXSLTExt().isEmpty()) &&
                        ( Message.MessageTemplate4Perform.getPropJavaMethodName() == null))
                    { // 2) EnvelopeXSLTExt !! => JDBC-обработчик

                        if (Message.MessageTemplate4Perform.getIsDebugged()) {
                            MessegeReceive_Log.info("[" + Queue_Id + "] Шаблон для SQL-XSLTExt-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() + ")");
                            if (Message.MessageTemplate4Perform.getIsExtSystemAccess()) {
                                MessegeReceive_Log.info("[" + Queue_Id + "] Шаблон для SQL-XSLTExt-обработки использует пулл коннектов для внешней системы");
                            }
                        }
                        String Passed_Envelope4XSLTExt = null;
                        try {
                            Passed_Envelope4XSLTExt = XMLutils.ConvXMLuseXSLT(Queue_Id,
                                    // Message.XML_MsgClear.toString(),
                                    MessageUtils.PrepareEnvelope4XSLTExt(messageQueueVO, Message.XML_Request_Method, MessegeReceive_Log), // Искуственный Envelope/Head/Body + XML_Request_Method
                                    Message.MessageTemplate4Perform.getEnvelopeXSLTExt(),  // через EnvelopeXSLTExt
                                    Message.MsgReason, // результат помещаем сюда
                                    ConvXMLuseXSLTerr,
                                    XSLTErrorListener,
                                    MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                        } catch (TransformerException exception) {
                            MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLTExt-преобразователь запроса:{" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() + "}");
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt(), 3229,
                                    MessegeReceive_Log);
                            return -31L;
                        }
                        if (Passed_Envelope4XSLTExt.equals(XMLchars.EmptyXSLT_Result)) {
                            MessegeReceive_Log.error("[" + Queue_Id + "] Шаблон для XSLTExt-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() + ")");
                            MessegeReceive_Log.error("[" + Queue_Id + "] Envelope4XSLTExt:" + ConvXMLuseXSLTerr);
                            MessegeReceive_Log.error("[" + Queue_Id + "] Ошибка преобразования XSLT для XSLTExt-обработки " + Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 3231,
                                    MessegeReceive_Log);
                            return -32L;

                        }
                        // проверяем, есть ли Java-обрабочик - конкретный метод, отличный от ExecuteSQLincludedXML, указанный в PropJavaMethodName
                        if ( Message.MessageTemplate4Perform.getPropJavaMethodName() == null)
                        {   // специального класса нет -  используем XmlSQLStatement.ExecuteSQLincludedXML
                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessegeReceive_Log.info("[" + Queue_Id + "] try ExecuteSQLincludedXML (" + Passed_Envelope4XSLTExt + ")");

                            int resultSQL;
                            if (Message.MessageTemplate4Perform.getIsExtSystemAccess()) {
                                ExtSystemDataConnection extSystemDataConnection = new ExtSystemDataConnection(Queue_Id, MessegeReceive_Log);
                                if ( extSystemDataConnection.ExtSystem_Connection == null ){
                                    Message.MsgReason.append("Ошибка на приёме сообщения - нет соединения с внешней базой данных (extSystemDataConnection return NULL), обратитесь к системному администратору !");
                                    return -33L;
                                }
                                resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, true, extSystemDataConnection.ExtSystem_Connection ,
                                                                                  Passed_Envelope4XSLTExt, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);
                                try {  extSystemDataConnection.ExtSystem_Connection.close(); } catch (SQLException e) {
                                    MessegeReceive_Log.error("[" + Queue_Id + "] ExtSystem_Connection.close() fault:" + e.getMessage());
                                }
                            }
                            else
                            resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, false, null, Passed_Envelope4XSLTExt, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);
                            if (resultSQL != 0) {
                                MessegeReceive_Log.error("[" + Queue_Id + "] Envelope4XSLTExt:" + ConvXMLuseXSLTerr);
                                MessegeReceive_Log.error("[" + Queue_Id + "] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString());
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 3231,
                                        MessegeReceive_Log);
                                return -34L;
                            } else {
                                if (Message.MessageTemplate4Perform.getIsDebugged())
                                    MessegeReceive_Log.info("[" + Queue_Id + "] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString());
                            }
                        }

                    }
                    else
                    {    // Нет Envelope4XSLTExt - надо орать!
                        MessegeReceive_Log.error("["+ Queue_Id +"] В шаблоне для XSLTExt-обработки " + Message.MessageTemplate4Perform.getTemplate_name() + " нет Envelope4XSLTExt");
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,
                                "В шаблоне для XSLTExt-обработки " + Message.MessageTemplate4Perform.getTemplate_name() + " нет Envelope4XSLTExt", 3233,
                                 MessegeReceive_Log);
                        Message.MsgReason.append( "В шаблоне для XSLTExt-обработки " ).append( Message.MessageTemplate4Perform.getTemplate_name() ).append( " нет Envelope4XSLTExt");
                        return -35L;
                    }
                }

                MessegeReceive_Log.warn("["+ Queue_Id +"] В шаблоне для синхронной обработки getIsDebugged="+ Message.MessageTemplate4Perform.getIsDebugged() +
                                        " getPropExeMetodExecute= " + Message.MessageTemplate4Perform.getPropExeMetodExecute() );
                if ( (Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) &&
                        ( ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.WebRestExeMetod) ) ||
                          ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.WebJsonExeMetod) ) )
                   )
                { // 2.2) Это Rest-HttpGet-вызов
                    if (( Message.MessageTemplate4Perform.getPropHost() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropUser() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropPswd() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropUrl()  == null ) )
                    {
                        // Нет параметров для Rest-HttpGet - надо орать!
                        Message.MsgReason.append( "В шаблоне для синхронной-обработки " ).append( Message.MessageTemplate4Perform.getPropExeMetodExecute() ).append( " недостаточно параметров для Rest-HttpGet вклюая host/url/логин/пароль");
                        MessegeReceive_Log.error("["+ Queue_Id +"] " + Message.MsgReason.toString());
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3242,
                               MessegeReceive_Log);
                        return -36L;
                    }
                    String EndPointUrl;
                    /* перейти на Java 11 HTTP Client*/
                    HttpClient ApiRestHttpClient = getCloseableHttpClient(  messageQueueVO,  Message ,  false,
                                                                            theadDataAccess, MessegeReceive_Log);
                    if ( ApiRestHttpClient == null) {
                        return -36L;
                    }
                    // Формируем URL для вызова Http-Get/?queue_id или Http-Post {JSON}
                    if ( StringUtils.substring(Message.MessageTemplate4Perform.getPropHost(),0,"http".length()).equalsIgnoreCase("http") )
                        EndPointUrl =  Message.MessageTemplate4Perform.getPropHost() +
                                       Message.MessageTemplate4Perform.getPropUrl();
                    else
                        EndPointUrl = "http://" + Message.MessageTemplate4Perform.getPropHost() +
                                                  Message.MessageTemplate4Perform.getPropUrl();

                    try {
                        String RestResponse=null;
                        if ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
                        { RestResponse = MessageHttpSend.WebRestExecGET(ApiRestHttpClient,EndPointUrl, Queue_Id, Message.MessageTemplate4Perform,
                                                                        ApplicationProperties.ApiRestWaitTime, MessegeReceive_Log);
                         }
                        if ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.WebJsonExeMetod) )
                        { // отправляем в Rest то, что получили внутри Soap
                            String jsonPrettyPrintString;
                            try {
                                //
                                JSONObject xmlJSONObj = XML.toJSONObject(Message.XML_Request_Method.toString());
                                jsonPrettyPrintString = xmlJSONObj.toString(2);
                                if (Message.MessageTemplate4Perform.getIsDebugged())
                                    MessegeReceive_Log.warn("[" + Queue_Id + "] JSON-HttpForvard готов:" + jsonPrettyPrintString);
                             }
                             catch (JSONException e) {
                            System.err.println("[" + Queue_Id + "] Не смогли преобразовать XML_Request_Method `" + Message.XML_Request_Method + "` в JSON: " + e);
                                 MessegeReceive_Log.error( "[" + Queue_Id + "] Не смогли преобразовать XML_Request_Method `" + Message.XML_Request_Method + "` в JSON: " + e.getMessage());
                                 theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка синхронного вызова обработчика: не смогли преобразовать XML в JSON:" + e.toString(), 3244,
                                         MessegeReceive_Log);
                                 return -38L;
                            }
                            RestResponse = MessageHttpSend.WebRestExecPOSTJSON(ApiRestHttpClient,EndPointUrl, Queue_Id, Message.MessageTemplate4Perform,
                                    ApplicationProperties.ApiRestWaitTime, jsonPrettyPrintString, MessegeReceive_Log);

                        }
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessegeReceive_Log.info("[" + Queue_Id + "] MetodExec." + Message.MessageTemplate4Perform.getPropExeMetodExecute() + ": `" + EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id)+ "` RestResponse=(" + RestResponse + ")");

                    } catch ( Exception e) {
                        System.err.println("[" + Queue_Id + "] Ошибка синхронного вызова обработчика "+ Message.MessageTemplate4Perform.getPropExeMetodExecute()+ "(" + EndPointUrl + ")" + e.getMessage() ); //e.printStackTrace();
                        // возмущаемся,
                        Message.MsgReason.append('[').append(Queue_Id ).append( "] Ошибка синхронного вызова REST обработчика (").append( EndPointUrl).append( "):" ).append( e );
                        MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка синхронного вызова REST обработчика "+ Message.MessageTemplate4Perform.getPropExeMetodExecute()+ "(" + EndPointUrl + "):" + e.toString() );
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка синхронного вызова обработчика  "+ Message.MessageTemplate4Perform.getPropExeMetodExecute()+ "(" + EndPointUrl + "):" + e.toString(), 3244,
                                MessegeReceive_Log);
                        try {
                            ApiRestHttpClient.close();

                        } catch ( Exception IOe) {
                            MessegeReceive_Log.error("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()..."+ IOe.getMessage() );
                            System.err.println("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()...\"" + IOe.getMessage() ); //IOe.printStackTrace();
                        }
                        return -37L;
                    }
                    try {
                        ApiRestHttpClient.close();
                    } catch ( Exception IOe) {
                        MessegeReceive_Log.error("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()..."+ IOe.getMessage() );
                        System.err.println("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()...\"" + IOe.getMessage() ); //IOe.printStackTrace();
                    }
                }

                if (Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) {
                    // Был обработчик, нужен результат
                    // Проверяем готовность результата
                    if (MessageUtils.isMessageQueue_Direction_EXEIN(theadDataAccess, Queue_Id, messageQueueVO, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log )) {
                        // Если статус EXEIN , сообщение выполнено, либо нормально либо с ошибкой смотрим на Confirmation
                        // если на интерфейсе в типе сообщения URL_SOAP_ACK = REST- это значит, что Confirmation в БД не пишем
                        is_NoConfirmation = // Признак на типе сообщения, что Confirmation формируется в памяти, messageDetails.XML_MsgConfirmation
                                MessageRepositoryHelper.isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation(messageQueueVO.getOperation_Id(), MessegeReceive_Log);
                       if ( !is_NoConfirmation ) { // Если признака "NoConfirmation" на типе сообщения нет, значит положено читать из БД
                           // ReadConfirmation очищает Message.XML_MsgConfirmation и помещает туда чстанный из БД Confirmation
                        int ConfirmationRowNum = MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, Message, MessegeReceive_Log);
                        if (ConfirmationRowNum < 1) {
                            // Ругаемся, что обработчик не сформировал Confirmation
                            Message.MsgReason.append("[" + Queue_Id + "] обработчик не сформировал Confirmation, нарушено соглашение о взаимодействии с Шиной");
                            MessegeReceive_Log.error("[" + Queue_Id + "] " + Message.MsgReason);
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3245,
                                    MessegeReceive_Log);
                            return -38L;
                        }
                      }
                    } else {
                        // Ругаемся, что обработчик не выставил признак статус EXEIN
                        Message.MsgReason.append("[" + Queue_Id + "] обработчик не выставил признак статус EXEIN , нарушено соглашение о взаимодействии с Шиной");
                        MessegeReceive_Log.error("[" + Queue_Id + "] " + Message.MsgReason);
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3247,
                                MessegeReceive_Log);
                        return -39L;
                    }
                    // Проверяем, создавалось ли OUT-сообщение в Синхронном обработчике
                    Link_Queue_Id = MessageUtils.get_SelectLink_Queue_Id(theadDataAccess, Queue_Id, messageQueueVO, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);
                }
                else
                { // Формируем псевдо XML_MsgConfirmation из
                    Message.XML_MsgConfirmation.setLength(0);
                    Message.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag
                            + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag
                            + "0"
                            + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag
                            + XMLchars.OpenTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag
                            + "Success"
                            + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag
                     + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag
                    );

                }
                if ( Link_Queue_Id != null) // Обрабатываем порожденное сообщение
                { // Проверяем в цикле периодически - спорадически готово ли OUT
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeReceive_Log.warn("[" + Queue_Id + "] Проверяем в цикле периодически - готово ли OUT Link_Queue_Id=" + Link_Queue_Id );
                    theadDataAccess.doUPDATE_MessageQueue_ExeIN2PostIN(  Queue_Id, "Ожидаем завершения обработки Q=" + Link_Queue_Id.toString() , MessegeReceive_Log);

                    int theadNum  = MessageUtils.get_SelectLink_msg_InfostreamId(theadDataAccess, Link_Queue_Id, Message.MessageTemplate4Perform.getIsDebugged(),  MessegeReceive_Log);
                    ////////////////////////////////
                    /// посылаем сообщение в Очередь - для ускорения !!!
                    String MessageDirectionsCode = null;
                    PerformTextMessageJMSQueue performTextMessageJMSQueue = new PerformTextMessageJMSQueue();
                    javax.jms.Connection Qconnection = null;
                    if ( theadNum > 0 ) {
                        MessageDirectionsCode = MessageRepositoryHelper.look4MessageDirectionsCode_4_Num_Thread(theadNum, MessegeReceive_Log);

                        //PerformTextMessageJMSQueue.JMSQueueContext QueueContext;
                        try {
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.info("[" + Queue_Id + "] Пробуем отправить сообщение QUEUE_ID: " + Link_Queue_Id + " в очередь сообщений ActiveMQ 'Q." + MessageDirectionsCode + ".IN'");

                            Qconnection = performTextMessageJMSQueue.SendTextMessageJMSQueue(
                                        "{ \"QUEUE_ID\": \"" + Link_Queue_Id.toString() + "\" }",
                                            "Q." + MessageDirectionsCode + ".IN",
                                                        StoreMQpooledConnectionFactory.MQpooledConnectionFactory
                            );
                        } catch (JMSException e) {
                            MessegeReceive_Log.warn("[" + Queue_Id + "] НЕ удалось отправить сообщение Link_Queue_Id: " + Link_Queue_Id + " в очередь сообщений ActiveMQ 'Q." + MessageDirectionsCode + ".IN', fault:" + e.getMessage());
                            Message.MsgReason.append(" НЕ удалось отправить сообщение Link_Queue_Id: " + Link_Queue_Id + " в очередь сообщений ActiveMQ 'Q." + MessageDirectionsCode + ".IN':" + e.getMessage());
                           // return Queue_Id;
                            Qconnection = null;
                        }
                        if (Qconnection != null)
                        {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.info("[" + Queue_Id + "] Отправили сообщение Link_Queue_Id: " + Link_Queue_Id + " в очередь сообщений ActiveMQ 'Q." + MessageDirectionsCode + ".IN'");
                            }
                    }

                    boolean isLink_Queue_Finish=false;
                    // # hermes.api-rest-wait-time=1200
                    int try_count = 0;
                    //int time4waitMessageReplyQueue=0; // Несльзя ждать весь тайм-аут на jms-QUEUE, т.к. запрос межет взять нет тот Sender, который прочимал сообщение)
                    Integer ShortRetryCountPostExec = Message.MessageTemplate4Perform.getShortRetryCountPostExec();
                    Integer ShortRetryIntervalPostExec = Message.MessageTemplate4Perform.getShortRetryIntervalPostExec();
                    Integer LongRetryCountPostExec  = Message.MessageTemplate4Perform.getLongRetryCountPostExec();
                    Integer LongRetryIntervalPostExec  = Message.MessageTemplate4Perform.getLongRetryIntervalPostExec();
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessegeReceive_Log.warn("[" + Queue_Id + "] Time-out calculate: ShortRetryCountPostExec=" + ShortRetryCountPostExec +
                                " ; ShortRetryIntervalPostExec=" + ShortRetryIntervalPostExec +
                                " ; LongRetryCountPostExec=" + LongRetryCountPostExec +
                                " ; LongRetryIntervalPostExec=" + LongRetryIntervalPostExec);
                    if ( ( LongRetryIntervalPostExec != null ) &&
                         ( LongRetryCountPostExec != null )) {
                        //time4waitMessageReplyQueue = LongRetryIntervalPostExec * LongRetryCountPostExec;
                        try_count = (LongRetryIntervalPostExec * LongRetryCountPostExec) / 2;
                    }
                    if ( ( ShortRetryCountPostExec != null ) &&
                            ( ShortRetryIntervalPostExec != null ) ) {
                        //time4waitMessageReplyQueue = time4waitMessageReplyQueue + ( ShortRetryIntervalPostExec * ShortRetryCountPostExec );
                        try_count = try_count  +
                                ( ShortRetryIntervalPostExec * ShortRetryCountPostExec ) / 2;
                    }

                    if (try_count == 0 )
                    {   //time4waitMessageReplyQueue = ApplicationProperties.ApiRestWaitTime/1000;
                        try_count = ApplicationProperties.ApiRestWaitTime/( 2 * 1000);
                        if  (try_count == 0 ) try_count = 1;
                    }
                    ///! try_count = 1;
                    int time4wait = try_count * 2;

                    while ((!isLink_Queue_Finish) && (try_count > 0)) {
                        try {
                            if ( Qconnection == null) // Если не удалось присоедениться к JMS
                            Thread.sleep( 2 * 1000);
                            else {
                                if ( performTextMessageJMSQueue.ReadTextMessageReplyQueue(2 * 1000, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log) != null )
                                 Qconnection = null; // сообщение зачитано, но если исходящее не готово( потому что его взял нет тот Sender, который прочимал сообщение), то надо проверять по Taine-out в цикле дальше
                            }
                            //////////////////////////////////////
                            try_count = try_count -1;
                        } catch ( JMSException | InterruptedException e) { //
                            Qconnection = null; // что-то не так с брокером, надо переходить на цикл с ожиданием sleep( 2 * 1000);
                            MessegeReceive_Log.error("Message ReadTextMessageReplyQueue() ExeIN2PostIN wait Task[" + Queue_Id + "]: is interrapted: " + e.getMessage());
                            System.err.println("Message ReadTextMessageReplyQueue() ExeIN2PostIN wait Task[" + Queue_Id + "]: is interrapted: ");
                            System.err.println(e.getMessage()); // .printStackTrace();
                        }
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessegeReceive_Log.warn("[" + Queue_Id + "] Начинаем проверять в цикле периодически - готово ли OUT, try_count=" + String.valueOf(try_count) + " в течении " + time4wait + " секунд" );

                        isLink_Queue_Finish = MessageUtils.isLink_Queue_Finish(theadDataAccess, Link_Queue_Id, Message.MessageTemplate4Perform.getIsDebugged(),  MessegeReceive_Log);
                        if (isLink_Queue_Finish) {

                            try_count = 0;
                        }
                    }
                    // останавливаем jms.Connection !
                    performTextMessageJMSQueue.Stop_and_Close_MessageJMSQueue( Queue_Id,  MessegeReceive_Log );

                    if ( isLink_Queue_Finish)
                    { // Считаем, что как то готово готово
                       // MessegeReceive_Log.error("[" + Queue_Id + "] Считаем, что как то готово готово, " + "AckAnswXSLT: " + Message.MessageTemplate4Perform.getAckAnswXSLT());
                        if (Message.MessageTemplate4Perform.getAckAnswXSLT() != null)
                        {
                            // надо читать ответ из Confirmation порожденного OUT
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.warn("[" + Queue_Id + "] AckAnswXSLT: " + Message.MessageTemplate4Perform.getAckAnswXSLT());
                            // ReadConfirmation очищает Message.XML_MsgConfirmation и помещает туда чстанный из БД Confirmation
                            int ConfirmationRowNum = MessageUtils.ReadConfirmation(theadDataAccess, Link_Queue_Id, Message, MessegeReceive_Log);
                            if (ConfirmationRowNum < 1) {
                                // Ругаемся, что обработчик не сформировал Confirmation
                                String Link_Queue_Direction = MessageUtils.get_Link_Queue_Finish(theadDataAccess, Link_Queue_Id, Message.XML_MsgConfirmation,
                                                                                    Message.MessageTemplate4Perform.getIsDebugged(),  MessegeReceive_Log);
                                if (( Link_Queue_Direction != null ) && (!Message.XML_MsgConfirmation.isEmpty()) )
                                    switch ( Link_Queue_Direction )
                                    { case XMLchars.DirectERROUT:
                                        Message.MsgReason.append("[" + Queue_Id + "] при взаимодействии с внешней система на событие (" + Link_Queue_Id + ") произошёл сбой " + Message.XML_MsgConfirmation );
                                            break;
                                        case XMLchars.DirectATTNOUT:
                                        case XMLchars.DirectDELOUT:
                                            Message.MsgReason.append("[" + Queue_Id + "] обработчик Исходящего события (" + Link_Queue_Id + ") не сформировал Confirmation, выствлен статус события ("+ Link_Queue_Direction +"), нарушено соглашение о взаимодействии с Шиной");
                                    }
                                else {
                                    Message.MsgReason.append("[" + Queue_Id + "] обработчик Исходящего события (" + Link_Queue_Id + ") не сформировал Confirmation, статус события неопределён, нарушено соглашение о взаимодействии с Шиной");

                                }
                                MessegeReceive_Log.error("[" + Queue_Id + "]" + Message.MsgReason.toString());
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3245,
                                        MessegeReceive_Log);
                                return -40L;
                            }
                            String Passed_Confirmation4AckAnswXSLT = null;
                            try {
                                Passed_Confirmation4AckAnswXSLT = XMLutils.ConvXMLuseXSLT(Queue_Id,
                                        Message.XML_MsgConfirmation.toString(), //
                                        Message.MessageTemplate4Perform.getAckAnswXSLT(),  // через AckAnswXSLT
                                        Message.MsgReason, // результат для MsgReason помещаем сюда
                                        ConvXMLuseXSLTerr,
                                        XSLTErrorListener,
                                        MessegeReceive_Log,
                                        true //Message.MessageTemplate4Perform.getIsDebugged()
                                );
                            } catch (TransformerException exception) {
                                MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLTExt-преобразователь Confirmation:{" + Message.MessageTemplate4Perform.getAckAnswXSLT() + "}");
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getAckAnswXSLT(), 3249,
                                        MessegeReceive_Log);
                                return -42L;
                            }
                            if (Passed_Confirmation4AckAnswXSLT.equals(XMLchars.EmptyXSLT_Result)) {
                                MessegeReceive_Log.error("[" + Queue_Id + "] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getAckAnswXSLT() + ")");
                                MessegeReceive_Log.error("[" + Queue_Id + "] Passed_Confirmation4AckAnswXSLT:" + ConvXMLuseXSLTerr);
                                MessegeReceive_Log.error("[" + Queue_Id + "] Ошибка преобразования XSLT для обработки Confirmation" + Message.MsgReason.toString());
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3251,
                                        MessegeReceive_Log);
                                return -43L;

                            }
                            Message.XML_MsgResponse.append(Passed_Confirmation4AckAnswXSLT.substring(XMLchars.xml_xml.length()));
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.warn("[" + Queue_Id + "] возврашаем от исходящего " + Link_Queue_Id + " сообщения (" + Message.XML_MsgResponse + ")");
                            // Устанавливаеи признак завершения работы
                            theadDataAccess.doUPDATE_MessageQueue_ExeIn2DelIN(Queue_Id, MessegeReceive_Log);
                            return  0L;
                        }

                        if ((Message.MessageTemplate4Perform.getConfigPostExec()!= null) && (Message.MessageTemplate4Perform.getMsgAnswXSLT() != null ))
                         { // Есть Post-обработчик , работающий ПОВЕРХ результата порожденного OUT-сообщения
                            // надо читать ответ из Confirmation родного OUT, куда дополнительный обработчик положит Confirmation, перезаписав его
                            // или обработчик порожденного OUT-сообщения перезаписывает Confirmation входящего по результатам прикладной обработки Confirmation от Link_Queue
                            /////////////////////////////////////////////////
                            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) ) { // 2.2) Это Rest-HttpGet-вызов

                                if ((Message.MessageTemplate4Perform.getPropHostPostExec() == null) ||
                                        (Message.MessageTemplate4Perform.getPropUserPostExec() == null) ||
                                        (Message.MessageTemplate4Perform.getPropPswdPostExec() == null) ||
                                        (Message.MessageTemplate4Perform.getPropUrlPostExec() == null) ||
                                        (Message.MessageTemplate4Perform.getPropQueryPostExec() == null) ) {
                                    // Нет параметров для Rest-HttpGet - надо орать!
                                    MessegeReceive_Log.error("[" + Queue_Id + "] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль");
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,
                                            "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль", 3253,
                                            MessegeReceive_Log);
                                    return -46L;
                                }
                                // Готовим Rest-call
                                String EndPointUrl = null;
                                HttpClient ApiRestHttpClient=null;
                                int restResponseStatus = 0;
                                // HashMap<String, String > HttpGetParams = new HashMap<String, String >();
                                try {

                                    if (StringUtils.substring(Message.MessageTemplate4Perform.getPropHostPostExec(), 0, "http".length()).equalsIgnoreCase("http"))
                                        EndPointUrl = Message.MessageTemplate4Perform.getPropHostPostExec() +
                                                      Message.MessageTemplate4Perform.getPropUrlPostExec();
                                    else
                                        EndPointUrl = "http://" + Message.MessageTemplate4Perform.getPropHostPostExec() +
                                                                  Message.MessageTemplate4Perform.getPropUrlPostExec();
                                    // Ставим своенго клиента !
                                    ApiRestHttpClient = getCloseableHttpClient( messageQueueVO,  Message, true, theadDataAccess, MessegeReceive_Log);
                                    // SSLUtil.turnOffSslChecking();
                                    if ( ApiRestHttpClient == null) // ErrIN выставлен, выходим
                                        return -36L;

                                    if (Message.MessageTemplate4Perform.getIsDebugged())
                                        MessegeReceive_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + " MessageTemplate4Perform.getPropQueryPostExec:'" + Message.MessageTemplate4Perform.getPropQueryPostExec() + "'");
                                    String queryEndPointUrl;
                                    if (Message.MessageTemplate4Perform.getPropQueryPostExec() != null)
                                        queryEndPointUrl = EndPointUrl + "?" + Message.MessageTemplate4Perform.getPropQueryPostExec() + "=" + Link_Queue_Id.toString() ;
                                    else queryEndPointUrl = EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id)+ "&Link_Queue_Id=" + Link_Queue_Id.toString() ;

                                    /*int numOfParams = MessageHttpSend.setHttpGetParams(messageQueueVO.getQueue_Id(),
                                                                                        Message.MessageTemplate4Perform.getPropQueryPostExec(), Link_Queue_Id.toString(),
                                                                                        HttpGetParams,
                                                                                        Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log );*/
                                        restResponseStatus =  MessageHttpSend.WebRestExePostExec(ApiRestHttpClient, queryEndPointUrl, Queue_Id,
                                                                                              Message.MessageTemplate4Perform, ApplicationProperties.ApiRestWaitTime, MessegeReceive_Log );

                                    try {
                                            ApiRestHttpClient.close();

                                    } catch ( Exception IOe) {
                                        MessegeReceive_Log.error( "[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()..."+ IOe.getMessage());
                                        System.err.println("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()...\" пост-обработки" + IOe.getMessage() ); // IOe.printStackTrace();
                                    }

                                } catch (Exception e ) { //  | java.security.KeyManagementException | java.security.NoSuchAlgorithmException  e) {
                                    // ???? возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать - что то не видно про "ResOUT"

                                    Message.MsgReason.append("[" + Queue_Id + "] Ошибка вызова пост-обработки HttpGet(" + EndPointUrl + "), статус[" +restResponseStatus + "]:" +e.getMessage() );
                                    MessegeReceive_Log.error("[" + Queue_Id + "] Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + e.toString());
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,
                                            "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTrace.strInterruptedException(e), 3255,
                                            MessegeReceive_Log);
                                    try {
                                        if ( ApiRestHttpClient != null)
                                            ApiRestHttpClient.close();

                                    } catch ( Exception IOe) {
                                        MessegeReceive_Log.error( "[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()..."+ IOe.getMessage());
                                        System.err.println("[" + Queue_Id + "] И ещё проблема с ApiRestHttpClient.close()...\" пост-обработки" + IOe.getMessage() ); //IOe.printStackTrace();
                                    }
                                    return -47L;
                                }
                                // Проверяем готовность результата
                                if (! MessageUtils.isMessageQueue_Direction_EXEIN(theadDataAccess, Queue_Id, messageQueueVO, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log ))
                                {
                                    // Ругаемся, что обработчик не выставил признак статус EXEIN
                                    Message.MsgReason.setLength(0); Message.MsgReason.trimToSize();
                                    Message.MsgReason.append("[" + Queue_Id + "] При вызове Post-обработчика HttpGet(" + EndPointUrl + ")статус[" +restResponseStatus + "]:не выставлен признак статус EXEIN, нарушено соглашение о взаимодействии с Шиной");
                                    MessegeReceive_Log.error( Message.MsgReason.toString());
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3248,
                                            MessegeReceive_Log);
                                    return -48L;
                                }
                            } // закончили Rest-Post-обработку
                              // обработчик порожденного OUT-сообщения перезаписывает Confirmation, его надо перезачитать и обработать MsgAnswXSLT

                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeReceive_Log.warn("[" + Queue_Id + "]: ожидается, что обработчик порожденного OUT-сообщения перезаписывает Confirmation, MsgAnswXSLT: " + Message.MessageTemplate4Perform.getMsgAnswXSLT());
                                // ReadConfirmation очищает Message.XML_MsgConfirmation и помещает туда чстанный из БД Confirmation
                                int ConfirmationRowNum = MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, Message, MessegeReceive_Log);
                                if (ConfirmationRowNum < 1) {
                                    // Ругаемся, что обработчик не сформировал Confirmation
                                    Message.MsgReason.append("[" + Queue_Id + "] обработчик порожденного Исходящего события не сформировал Confirmation, нарушено соглашение о взаимодействии с Шиной");
                                    MessegeReceive_Log.error( Message.MsgReason.toString());
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3245,
                                            MessegeReceive_Log);
                                    return -49L;
                                }
                                String Passed_Confirmation4MsgAnswXSLT = null;
                                try {
                                    Passed_Confirmation4MsgAnswXSLT = XMLutils.ConvXMLuseXSLT(Queue_Id,
                                            Message.XML_MsgConfirmation.toString(), //
                                            Message.MessageTemplate4Perform.getMsgAnswXSLT(),  // через AckAnswXSLT
                                            Message.MsgReason, // результат для MsgReason помещаем сюда
                                            ConvXMLuseXSLTerr,
                                            XSLTErrorListener,
                                            MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                                } catch (TransformerException exception) {
                                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLTExt-преобразователь Confirmation:{" + Message.MessageTemplate4Perform.getMsgAnswXSLT() + "}");
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MessageTemplate4Perform.getMsgAnswXSLT(), 3249,
                                            MessegeReceive_Log);
                                    return -50L;
                                }
                                if (Passed_Confirmation4MsgAnswXSLT.equals(XMLchars.EmptyXSLT_Result)) {
                                    MessegeReceive_Log.error("[" + Queue_Id + "] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getMsgAnswXSLT() + ")");
                                    MessegeReceive_Log.error("[" + Queue_Id + "] Passed_Confirmation4AckAnswXSLT:" + ConvXMLuseXSLTerr);
                                    MessegeReceive_Log.error("[" + Queue_Id + "] Ошибка преобразования XSLT для обработки Confirmation" + Message.MsgReason.toString());
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3251,
                                            MessegeReceive_Log);
                                    return -51L;

                                }
                                Message.XML_MsgResponse.append(Passed_Confirmation4MsgAnswXSLT.substring(XMLchars.xml_xml.length()));
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeReceive_Log.warn("[" + Queue_Id + "] возврашаем от исходящего " + Link_Queue_Id + " сообщения (" + Message.XML_MsgResponse + ")");
                                // Устанавливаеи признак завершения работы
                                theadDataAccess.doUPDATE_MessageQueue_ExeIn2DelIN(Queue_Id, MessegeReceive_Log);
                                return  0L;


                            /////////////////////////////////////////////////
                        }
                    }
                    else {
                        // Ругаемся, что исходяее сообщение не отработало за отведенное на это время
                        Message.MsgReason.append("[" + Queue_Id + "] - исходящее сообщение (" + Link_Queue_Id +") не отработало за отведенное на это время (" + time4wait + ") с. [Msg_Status=3244]");
                        MessegeReceive_Log.error("[" + Queue_Id + "] " + Message.MsgReason);
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3244,
                                MessegeReceive_Log);
                        return -51L;

                    }

                }

                // преобразовываем результат
                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                MessegeReceive_Log.warn("[" + Queue_Id + "] преобразовываем результат getAckXSLT( " + Message.MessageTemplate4Perform.getAckXSLT() +")");
                if (Message.MessageTemplate4Perform.getAckXSLT() != null)
                {
                    String Passed_Confirmation4AckXSLT = null;
                    try {
                        Passed_Confirmation4AckXSLT= XMLutils.ConvXMLuseXSLT( Queue_Id,
                                Message.XML_MsgConfirmation.toString(), //
                                Message.MessageTemplate4Perform.getAckXSLT(),  // через AckXSLT
                                Message.MsgReason, // результат для MsgReason помещаем сюда
                                ConvXMLuseXSLTerr,
                                XSLTErrorListener,
                                MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                    } catch ( TransformerException exception ) {
                        MessegeReceive_Log.error("[" + Queue_Id + "] " + Queue_Direction + " XSLTExt-преобразователь Confirmation:{" + Message.MessageTemplate4Perform.getAckXSLT() +"}");
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MessageTemplate4Perform.getAckXSLT(), 3249,
                                MessegeReceive_Log);
                        return -52L;
                    }
                    if ( Passed_Confirmation4AckXSLT.equals(XMLchars.EmptyXSLT_Result))
                    {   MessegeReceive_Log.error("["+ Queue_Id +"] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getAckXSLT() + ")");
                        MessegeReceive_Log.error("["+ Queue_Id +"] Passed_Confirmation4AckXSLT:" + ConvXMLuseXSLTerr);
                        MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для обработки Confirmation" + Message.MsgReason.toString() );
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3251,
                                MessegeReceive_Log);
                        return -53L;

                    }
                    Message.XML_MsgResponse.append(Passed_Confirmation4AckXSLT.substring(XMLchars.xml_xml.length()));
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeReceive_Log.warn("["+ Queue_Id +"] возврашаем(" + Message.XML_MsgResponse + ")");
                }
                else // Помещаем Без преобразования
                {
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeReceive_Log.warn("["+ Queue_Id +"] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getAckXSLT() + ")");
                    Message.XML_MsgResponse.append(Message.XML_MsgConfirmation.toString());
                }

                // проверяем НАЛИЧИЕ пост-обработчика в Шаблоне
                if (( Message.MessageTemplate4Perform.getConfigPostExec() != null ) &&
                    ( Message.MessageTemplate4Perform.getPropExeMetodPostExec()  != null )){ // 1) ConfigPostExec не пуст и обозначен Метод!
                    messageQueueVO.setQueue_Direction(XMLchars.DirectPOSTIN);
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
                    { // 2.1) Это JDBC-обработчик
                        if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost() != null ) { // 2) EnvelopeXSLTPost
                            if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost().length() > 0 ) {
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeReceive_Log.info("["+ Queue_Id +"] Шаблон EnvelopeXSLTPost для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeReceive_Log.info("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO, Message.XML_MsgConfirmation) );

                                String Passed_Envelope4XSLTPost;
                                try {
                                    Passed_Envelope4XSLTPost= XMLutils.ConvXMLuseXSLT( messageQueueVO.getQueue_Id(),
                                            MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO, Message.XML_MsgConfirmation),  // Искуственный Envelope/Head/<Body>XML_MsgConfirmation</Body>
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  // через EnvelopeXSLTPost
                                            Message.MsgReason, // результат для MsgReason помещаем сюда
                                            ConvXMLuseXSLTerr,
                                            XSLTErrorListener,
                                            MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                                } catch ( TransformerException exception ) {
                                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-пост-преобразователь ответа:{" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() +"}");
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN( messageQueueVO.getQueue_Id(),
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(), 1235,
                                              MessegeReceive_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeReceive_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessegeReceive_Log),
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  monitoringQueueVO, MessegeReceive_Log);
                                    return -101L;
                                }
                                if ( Passed_Envelope4XSLTPost.equals(XMLchars.EmptyXSLT_Result))
                                {   MessegeReceive_Log.error("["+ Queue_Id +"] Шаблон для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                    MessegeReceive_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost(messageQueueVO, Message.XML_MsgConfirmation) );
                                    MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для пост-обработки " + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(messageQueueVO.getQueue_Id(),
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1236,
                                             MessegeReceive_Log);
                                    return -102L;

                                }
                                /*
                                final int resultSQL = //XmlSQLStatement.ExecuteSQLincludedXML( theadDataAccess, Passed_Envelope4XSLTPost, messageQueueVO, Message, MessegeReceive_Log);
                                    XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, false, null, Passed_Envelope4XSLTPost, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);
                                */
                            int resultSQL;
                            if (Message.MessageTemplate4Perform.getIsExtSystemAccessPostExec()) {
                                ExtSystemDataConnection extSystemDataConnection = new ExtSystemDataConnection(Queue_Id, MessegeReceive_Log);
                                if ( extSystemDataConnection.ExtSystem_Connection == null ){
                                    Message.MsgReason.append("Ошибка на приёме сообщения - нет соединения с внешней базой данных (extSystemDataConnection return NULL), обратитесь к системному администратору !");
                                    return -33L;
                                }
                                resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, true, extSystemDataConnection.ExtSystem_Connection ,
                                                                                  Passed_Envelope4XSLTPost, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);
                                try {  extSystemDataConnection.ExtSystem_Connection.close(); } catch (SQLException e) {
                                    MessegeReceive_Log.error("[" + Queue_Id + "] ExtSystem_Connection.close() fault:" + e.getMessage());
                                }
                            }
                            else
                            resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, false, null, Passed_Envelope4XSLTPost, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessegeReceive_Log);


                                if (resultSQL != 0) {
                                    MessegeReceive_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO, Message.XML_MsgConfirmation) );
                                    MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(messageQueueVO.getQueue_Id(),
                                            "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1233,
                                              MessegeReceive_Log);
                                    return -103L;
                                }
                                else
                                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeReceive_Log.info("["+ Queue_Id +"] Исполнение (для пост-обработки) ExecuteSQLincludedXML() :=" + resultSQL );
                                }
                            }
                            else
                            {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                                MessegeReceive_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(messageQueueVO.getQueue_Id(),
                                        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1234,
                                          MessegeReceive_Log);
                                return -104L;
                            }
                        }
                        else
                        { // Нет EnvelopeXSLTPost - надо орать!
                            MessegeReceive_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(messageQueueVO.getQueue_Id(),
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1237,
                                      MessegeReceive_Log);
                            return -105L;
                        }
                    }
                }
                // Устанавливаеи признак завершения работы
                theadDataAccess.doUPDATE_MessageQueue_ExeIn2DelIN(Queue_Id, MessegeReceive_Log );

                break;
        }
            return  0L;
    }

    public HttpClient getCloseableHttpClient( MessageQueueVO messageQueueVO, MessageDetails Message , boolean isPostExec,TheadDataAccess theadDataAccess,
                                                       Logger MessegeReceive_Log) {
        // int ReadTimeoutInMillis = ApplicationProperties.ApiRestWaitTime * 1000;
        int ConnectTimeout = 5 ;
        SSLContext sslContext = MessageHttpSend.getSSLContext( Message.MsgReason );
        if ( sslContext == null ) {
            MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()+"] " + "SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")");
            Message.MsgReason.append("Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")" ) ;

            MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                    "Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")",
                    null, MessegeReceive_Log);
            return null;
        }
        boolean IsDebugged = Message.MessageTemplate4Perform.getIsDebugged();
        HttpClient ApiRestHttpClient;

        String PropUser;
        String PropPswd;
        if ( isPostExec ) {
            PropUser= Message.MessageTemplate4Perform.getPropUserPostExec();
            PropPswd = Message.MessageTemplate4Perform.getPropPswdPostExec();
        }
        else {
            PropUser = Message.MessageTemplate4Perform.getPropUser();
            PropPswd = Message.MessageTemplate4Perform.getPropPswd();
        }
   try {
        if ( (PropUser!= null)
               // && (!Message.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
            )
        {
            RestPasswordAuthenticator restPasswordAuthenticator = new RestPasswordAuthenticator();
            Authenticator restApiPasswordAuthenticator  = restPasswordAuthenticator.getPasswordAuthenticator(PropUser, PropPswd);
            if ( IsDebugged ) {
                MessegeReceive_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
            }
            ApiRestHttpClient = HttpClient.newBuilder()
                    .authenticator( restApiPasswordAuthenticator )
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds( ConnectTimeout ) )
                    .build();
        }
        else {
            if ( IsDebugged )
                MessegeReceive_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST PropUser== null (`" + PropUser + "`)" );
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds( ConnectTimeout ))
                    .build();
        }

     } catch ( Exception e)
      {
              MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()  +"] " + "httpClientBuilder.build() fault");
              Message.MsgReason.append("Внутренняя Ошибка httpClientBuilder.build() fault");

              MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                      "Внутренняя Ошибка httpClientBuilder.build() fault",
                      null, MessegeReceive_Log);
              return null;
               // MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()...");
               // System.err.println("["+ messageQueueVO.getQueue_Id()  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()..." + e.getMessage()); //e.printStackTrace();
      }


        return ApiRestHttpClient  ;
   }
/*
    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }
*/

}
