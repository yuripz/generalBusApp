package net.plumbing.msgbus.controller;


import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.jdom2.input.JDOMParseException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import javax.xml.xpath.XPathExpressionException;


import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import net.plumbing.msgbus.threads.utils.XMLutils;
import net.plumbing.msgbus.common.XMLchars;

public class RestAPI_ReceiveTask {

    public static final Logger RestAPI_Receive_Log = LoggerFactory.getLogger(MessageReceiveTask.class);

    // private ThreadSafeClientConnManager externalConnectionManager;
    private xlstErrorListener XSLTErrorListener=null;
    public TheadDataAccess theadDataAccess=null;
    private final String EventInitiator = "HRMS";


    // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public Long  ProcessRestAPIMessage(Integer Interface_id , MessageDetails Message, // контейнер сообщения для обработкиMessage.XML_MsgInput - заполнен входящими данными <Envelope/>
                                       int MessageOperationId, // номер операции, получаем из заголовка
                                       // int MessageTemplateVOkey, // индекс интерфейсного Шаблона
                                       boolean isDebugged) {

        XSLTErrorListener = new xlstErrorListener();
        StringBuilder ConvXMLuseXSLTerr = new StringBuilder(); ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        XSLTErrorListener.setXlstError_Log( RestAPI_Receive_Log );
        final  String Queue_Direction="ProcessRestAPIMessage";
        Long Queue_Id = -1L;
        Long Function_Result = 0L;
        Message.XML_MsgClear.setLength(0); Message.XML_MsgClear.trimToSize();
        Message.XML_MsgClear.append(  Message.XML_MsgInput );
        int MessageOperationTemplateVOkey; // индекс  Шаблона на входную операцию
        int MsgDirectionVO_Key =
                MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventInitiator, RestAPI_Receive_Log );
        if ( MsgDirectionVO_Key < 0 ) {
            Message.MsgReason.append("Ошибка на приёме сообщения:  в SOAP-заголовке (" +
                    XMLchars.TagEventInitiator + ") объявлен неизвестый код системы-инициатора " + EventInitiator);
            return -1L;
        }
;
        // ищем Шаблон под оперрацию, с учетом системы приёмника MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        MessageOperationTemplateVOkey = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(MessageOperationId,
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod(),
                RestAPI_Receive_Log);
        RestAPI_Receive_Log.info("`" + Queue_Direction + "`: look4MessageTemplateVO_2_Perform Шаблон под оперрацию (" +
                MessageOperationId + "), с учетом системы приёмника MsgDirection_Id=" +
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id() + ", SubSys_Cod =" +
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod() + " : вернул Template_Id="  + MessageOperationTemplateVOkey);

        //MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "]  Шаблон под оперрацию =" + Template_Id);
        String EnvelopeInXSLT = MessageTemplate.AllMessageTemplate.get( MessageOperationTemplateVOkey ).getEnvelopeInXSLT();
        if ( EnvelopeInXSLT != null ) {
            RestAPI_Receive_Log.info("`" + Queue_Direction + "`: MessageXSLT Шаблон под оперрацию `" + EnvelopeInXSLT + "`");
        }
            else {
            RestAPI_Receive_Log.info("`" + Queue_Direction + "`: MessageXSLT Шаблон под оперрацию `-NULL-`");
        }

        // MessageTemplateVOkey - Шаблон интерфейса (на основе входного URL), для конвертации боду надо использовать имеено его, т.к. получить шаблон оперрации с учетом получателя
        try {
                XMLutils.makeMessageDetailsRestApi(Message, EnvelopeInXSLT,
                         XSLTErrorListener, ConvXMLuseXSLTerr,
                         isDebugged,
                         RestAPI_Receive_Log);
        }
        catch (Exception e) {
            System.err.println( "["+ Message.XML_MsgInput + "]  Exception" );
            e.printStackTrace();
            RestAPI_Receive_Log.error(Queue_Direction + "fault: [" + Message.XML_MsgInput + "] XMLutils.makeClearRequest fault: " + sStackTracе.strInterruptedException(e));
            Message.MsgReason.append("Ошибка на приёме сообщения: " + e.getMessage() ); //  sStackTracе.strInterruptedException(e));
            if ( (e instanceof JDOMParseException ) || (e instanceof XPathExpressionException)  ) // Клиент прислсл фуфло
                return 1L;
            else
                return -1L;

        }
        if ( isDebugged )
            RestAPI_Receive_Log.info("Clear request:" + Message.XML_MsgClear.toString() );

        MessageQueueVO messageQueueVO = new MessageQueueVO();

        // TheadDataAccess
        this.theadDataAccess = new TheadDataAccess();
        // Установаливем " соединение" , что бы зачитывать очередь
        //  theadDataAccess.setDbSchema( ApplicationProperties.HrmsSchema ); - перенесён в make_Hikari_Connection(), что бы не забылось нигде!
        if ( isDebugged )
            RestAPI_Receive_Log.info("Установаливем \"соединение\" , что бы зачитывать очередь: [" +
                    ApplicationProperties.HrmsPoint + "] user:" + ApplicationProperties.hrmsDbLogin +
                    "; passwd:" + ApplicationProperties.hrmsDbPasswd + ".");
        theadDataAccess.make_Hikari_Connection(
                ApplicationProperties.HrmsSchema,
                ApplicationProperties.hrmsDbLogin,
                ApplicationProperties.dataSource,
                RestAPI_Receive_Log
        );
        if ( theadDataAccess.Hermes_Connection == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения - theadDataAccess.make_Hikari_Connection return: NULL!"  );
            return -2L;
        }

        // Создаем запись в таблице-очереди  select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL ...
        Queue_Id = MessageUtils.MakeNewMessage_Queue( messageQueueVO, theadDataAccess, RestAPI_Receive_Log );
        if ( Queue_Id == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения, не удалось сохранить заголовок сообщения в БД - MakeNewMessage_Queue return: " + Queue_Id );
            return -3L;
        }
        Message.ROWID_QUEUElog=null; Message.Queue_Id = Queue_Id;
        if ( isDebugged )
            Message.ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( Queue_Id , Message.XML_MsgInput, RestAPI_Receive_Log );

        try { // Парсим заголовок - получаем атрибуты messageQueueVO для сохранения в БД
            // XMLutils.Soap_HeaderRequest2messageQueueVO(Message.Soap_HeaderRequest.toString(), messageQueueVO, RestAPI_Receive_Log);

            MsgDirectionVO_Key =
                    MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventInitiator, RestAPI_Receive_Log );
            if ( MsgDirectionVO_Key < 0 ) {
                throw new XPathExpressionException("RestAPI_Receiver after ClearRequest: в SOAP-заголовке (" + XMLchars.TagEventInitiator + ") объявлен неизвестый код системы-инициатора " + EventInitiator);
            }
                messageQueueVO.setEventInitiator( MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                                                  MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());



            int MessageTypeVO_Key =
                    MessageRepositoryHelper.look4MessageTypeVO_2_Perform(  MessageOperationId , RestAPI_Receive_Log);
            if ( MessageTypeVO_Key >= 0 ) {
                messageQueueVO.setMsg_Type( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() );
                messageQueueVO.setMsg_Type_own( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own() );
                messageQueueVO.setOperation_Id( MessageOperationId );
                messageQueueVO.setOutQueue_Id( Queue_Id );
                messageQueueVO.setMsg_Reason( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() + "() Ok." );

                if ( isDebugged )
                RestAPI_Receive_Log.info("[" + Queue_Id + "] Нашли по (" + MessageTypeVO_Key + ") Msg_Type =`" +
                        MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() +
                        "`,  Msg_Type_ow=`" + MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own() + "`" );
            }
            else {
                RestAPI_Receive_Log.error( "RestAPI_Receiver after ClearRequest: в HTTP-заголовке (BusOperationId ) объявлен неизвестый № операцмм " + MessageOperationId);
                throw new XPathExpressionException("RestAPI_Receiver after ClearRequest: в HTTP-заголовке (BusOperationId) объявлен неизвестый № операцмм " + MessageOperationId + " ") ;
            }

        }
        catch (Exception e) {
            System.err.println( "Queue_Id["+ messageQueueVO.getQueue_Id() + "]  Exception" );
            System.err.println( sStackTracе.strInterruptedException( e ) );
            RestAPI_Receive_Log.error(Queue_Direction + " fault: [" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: " + sStackTracе.strInterruptedException(e));
            Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, fault: " + sStackTracе.strInterruptedException(e));

            MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                    "Ошибка при получении необходимых значений из заголовка : " + Message.XML_MsgClear.toString(),
                    e, RestAPI_Receive_Log);
            // Считаем, что виноват клиент
            return 7L;

        }

/*
        if ( MessageTemplateVOkey >= 0 )
        {  // Получаем Шаблон формирования заголовка для этого интерфейса HeaderInXSLT
            String MessageXSLT_4_HeaderIn = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getHeaderInXSLT();
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
                            RestAPI_Receive_Log,
                            isDebugged
                            ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    //if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    if ( isDebugged )
                        RestAPI_Receive_Log.info(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    if ( Message.Soap_HeaderRequest.toString().equals(XMLchars.nanXSLT_Result) ) {
                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess, "В результате XSLT преобразования получен пустой заголовок из (" + Message.XML_MsgClear.toString() + ")",
                                null, RestAPI_Receive_Log);
                        Message.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                        return -5L;
                    }

                } catch (TransformerException exception) {
                    RestAPI_Receive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_HeaderIn + "}");
                    RestAPI_Receive_Log.error(Queue_Direction + " [" + Queue_Id + "] fault " + ConvXMLuseXSLTerr.toString() + " после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " + ConvXMLuseXSLTerr + Message.XML_MsgClear.toString() + " on " + MessageXSLT_4_HeaderIn,
                            null, RestAPI_Receive_Log);
                    Message.MsgReason.append("Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " +  ConvXMLuseXSLTerr.toString());
                    // Считаем, что виноват клиент
                    return 5L;
                }

                try { // Парсим заголовок - получаем атрибуты messageQueueVO для сохранения в БД
                    XMLutils.Soap_HeaderRequest2messageQueueVO(Message.Soap_HeaderRequest.toString(), messageQueueVO, RestAPI_Receive_Log);

                }
                catch (Exception e) {
                    System.err.println( "Queue_Id["+ messageQueueVO.getQueue_Id() + "]  Exception" );
                    e.printStackTrace();
                    RestAPI_Receive_Log.error(Queue_Direction + "fault: [" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: " + sStackTracе.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString() + ", fault: " + sStackTracе.strInterruptedException(e));

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString(),
                            e, RestAPI_Receive_Log);
                    // Считаем, что виноват клиент
                    return 7L;

                }

            }
            else { // Берем распарсенный  Context из Header сообщения для сохранения в БД
                if ( Message.Input_Header_Context != null ) {
                    try {
                        XMLutils.Soap_XMLDocument2messageQueueVO(Message.Input_Header_Context, messageQueueVO, RestAPI_Receive_Log);
                    } catch (Exception e) {
                        System.err.println("Queue_Id [" + messageQueueVO.getQueue_Id() + "]  Exception");
                        e.printStackTrace();
                        RestAPI_Receive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTracе.strInterruptedException(e));
                        Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Queue_Direction + ", fault: " + sStackTracе.strInterruptedException(e));

                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                                "Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                                e, RestAPI_Receive_Log);
                        // Считаем, что виноват клиент
                        return 9L;

                    }
                }
                else {
                    RestAPI_Receive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_XMLDocument2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " );
                    Message.MsgReason.append("Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения (: " + Message.XML_MsgClear.toString() + ")" );

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                            null, RestAPI_Receive_Log);
                    // Считаем, что виноват клиент
                    return 11L;
                }
            }

        }
*/

        Message.XML_MsgResponse.setLength(0); Message.XML_MsgResponse.trimToSize();

        PerfotmInputMessages Perfotmer = new PerfotmInputMessages();
        Message.ReInitMessageDetails() ; // sslContext, httpClientBuilder, null, ApiRestHttpClient );
        try {

            // Обрабатываем сообщение!
            Function_Result = Perfotmer.performMessage(Message, messageQueueVO, theadDataAccess,
                                                       XSLTErrorListener,  ConvXMLuseXSLTerr,  RestAPI_Receive_Log );

        }
        catch (Exception e) {
            System.err.println( "performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            e.printStackTrace();
            RestAPI_Receive_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            RestAPI_Receive_Log.error( "что то пошло совсем не так...");
            MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                    "performMessage  fault:"  + e.getMessage() + " " + Message.XML_MsgClear.toString()  ,
                    null ,  RestAPI_Receive_Log);

        }

        // RestAPI_Receive_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
        // this.SenderExecutor.shutdown();

        return Function_Result.longValue(); // messageQueueVO.getQueue_Id();
    }

}
