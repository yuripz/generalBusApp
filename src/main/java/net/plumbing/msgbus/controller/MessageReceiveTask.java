package net.plumbing.msgbus.controller;

import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import net.plumbing.msgbus.threads.utils.XMLutils;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Xslt30Transformer;
import net.sf.saxon.s9api.XsltCompiler;
import org.jdom2.input.JDOMParseException;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;

import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

@Component
@Scope("prototype")
//@Configuration
//@Bean (name="MessageSendTask")
public class MessageReceiveTask
{
    //public Connection Hermes_Connection;

    public static final Logger MessegeReceive_Log = LoggerFactory.getLogger(MessageReceiveTask.class);

    // private ThreadSafeClientConnManager externalConnectionManager;
    private xlstErrorListener XSLTErrorListener=null;
    public TheadDataAccess theadDataAccess=null;



   // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public Long  ProcessInputMessage(Integer Interface_id , MessageDetails Message, // контейнер сообщения для обработкиMessage.XML_MsgInput - заполнен входящими данными <Envelope/>
                                     int MessageTemplateVOkey, // индекс интерфейсного Шаблона
                                     boolean isDebugged) {

        XSLTErrorListener = new xlstErrorListener();

        StringBuilder ConvXMLuseXSLTerr = new StringBuilder(); ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        XSLTErrorListener.setXlstError_Log( MessegeReceive_Log );
        final  String Queue_Direction="ProcessInputMessage";
        Long Queue_Id = -1L;
        Long Function_Result = 0L;

        MessageQueueVO messageQueueVO = new MessageQueueVO();

        // TheadDataAccess
        this.theadDataAccess = new TheadDataAccess();
        // Установаливем " соединение" , что бы зачитывать очередь
        //  theadDataAccess.setDbSchema( ApplicationProperties.HrmsSchema ); - перенесён в make_Hikari_Connection(), что бы не забылось нигде!
        if ( isDebugged )
            MessegeReceive_Log.info("Установаливем `соединение`, что бы зачитывать очередь: [{}] user:{}; passwd:{} Schema: {}.",
                    ApplicationProperties.HrmsPoint, ApplicationProperties.hrmsDbLogin, ApplicationProperties.hrmsDbPasswd, ApplicationProperties.HrmsSchema);
        theadDataAccess.make_Hikari_Connection(
                ApplicationProperties.HrmsSchema,
                ApplicationProperties.hrmsDbLogin,
                ApplicationProperties.dataSource,
                MessegeReceive_Log
        );
        if ( theadDataAccess.Hermes_Connection == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения - theadDataAccess.make_Hikari_Connection return: NULL!"  );
            return -2L;
        }
        // MessageTemplateVOkey - Шаблон интерфейса (на основе входного URL)
        if ( isDebugged )
            MessegeReceive_Log.info("{}: check content of Interface Template by MessageTemplateVOkey 4 getEnvelopeInXSLT(`{}`)", Queue_Direction, MessageTemplateVOkey);
        try {
            if ( MessageTemplateVOkey >= 0 )
            // Парсим входной запрос и формируем XML-Document !
            XMLutils.makeClearRequest(Message, MessageTemplateVOkey,
                        ConvXMLuseXSLTerr, isDebugged,
                    MessegeReceive_Log);
            else
                XMLutils.makeClearRequest(Message, -1,
                        ConvXMLuseXSLTerr, isDebugged,
                        MessegeReceive_Log);
        }
        catch (Exception e) {
            System.err.println( "["+ Message.XML_MsgInput + "]  Exception" );
            e.printStackTrace();
            MessegeReceive_Log.error("{} fault:`{}` XMLutils.makeClearRequest fault: {}", Queue_Direction, Message.XML_MsgInput, sStackTrace.strInterruptedException(e));
            Message.MsgReason.append("Ошибка на приёме сообщения: ").append(e.getMessage()); //  sStackTrace.strInterruptedException(e));
               if ( (e instanceof JDOMParseException ) || (e instanceof XPathExpressionException)  ) // Клиент прислсл фуфло
                   return 1L;
            else
                   return -1L;

        }
        if ( isDebugged )
            MessegeReceive_Log.info("Clear request: `{}` MessageTemplateVOkey 4 getEnvelopeInXSLT()={}", Message.XML_MsgClear.toString(), MessageTemplateVOkey);

        // Создаем запись в таблице-очереди  select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL ...
        Queue_Id = MessageUtils.MakeNewMessage_Queue( messageQueueVO, theadDataAccess, MessegeReceive_Log );
        if ( Queue_Id == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения, не удалось сохранить заголовок сообщения в БД - MakeNewMessage_Queue return: ").append(Queue_Id);
            return -3L;
        }
        // MessegeReceive_Log.info(" isDebugged ?:(" + isDebugged + ") theadDataAccess.doINSERT_QUEUElog(" + Queue_Id.toString() + ") ");
        Message.ROWID_QUEUElog=null; Message.Queue_Id = Queue_Id;
        if ( isDebugged )
            Message.ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( Queue_Id , Message.XML_MsgInput, MessegeReceive_Log );


        if ( MessageTemplateVOkey >= 0 )
        {  // Получаем Шаблон формирования заголовка для этого интерфейса HeaderInXSLT
            String MessageXSLT_4_HeaderIn = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getHeaderInXSLT();
            if ( isDebugged )
                MessegeReceive_Log.info(" [{}] {} MessageXSLT_4_HeaderIn= MessageTemplate.AllMessageTemplate.get{{}}", Queue_Id, Queue_Direction  ,MessageXSLT_4_HeaderIn);
            // При наличии в шаблоне интерфейса файла преобразования HeaderInXSLT,
            //  к исходному запросу(SOAP-Envelope) с удаленной информацией о NameSpace
            //  применяется преобразование HeaderInXSLT и полученный результат
            //  заменяет SOAP-Header запроса, вне зависимости от того был он или нет.
            if ( (MessageXSLT_4_HeaderIn != null) && (!MessageXSLT_4_HeaderIn.isEmpty()) )
            {
                Processor xslt30Processor = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getHeaderInXSLT_processor();
                XsltCompiler XsltCompiler = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getHeaderInXSLT_xsltCompiler();
                Xslt30Transformer Xslt30Transformer = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getHeaderInXSLT_xslt30Transformer();
                ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
                try {
                    Message.Soap_HeaderRequest.append(XMLutils.ConvXMLuseXSLT30(Queue_Id,
                            Message.XML_MsgClear.toString(),
                            xslt30Processor, XsltCompiler, Xslt30Transformer,
                            MessageXSLT_4_HeaderIn,
                            Message.MsgReason,
                            ConvXMLuseXSLTerr,
                            // XSLTErrorListener,
                            MessegeReceive_Log,
                            isDebugged
                            ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    //if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    if ( isDebugged )
                        MessegeReceive_Log.info( " [{}] {} после XSLT=:{{}}", Queue_Id, Queue_Direction, Message.Soap_HeaderRequest.toString());
                    if ( Message.Soap_HeaderRequest.toString().equals(XMLchars.nanXSLT_Result) ) {
                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess, "В результате XSLT преобразования получен пустой заголовок из (" + Message.XML_MsgClear.toString() + ")",
                                null, MessegeReceive_Log);
                        Message.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                        return -5L;
                    }

                } catch (SaxonApiException exception) {
                    MessegeReceive_Log.error("[{}] {} XSLT-преобразователь тела:{{}}", Queue_Id, Queue_Direction, MessageXSLT_4_HeaderIn);
                    MessegeReceive_Log.error("[{}] {} fault {} после XSLT=:{{}}", Queue_Id, Queue_Direction, ConvXMLuseXSLTerr, Message.Soap_HeaderRequest.toString());
                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " + ConvXMLuseXSLTerr + Message.XML_MsgClear.toString() + " on " + MessageXSLT_4_HeaderIn,
                            null, MessegeReceive_Log);
                    Message.MsgReason.append("Ошибка построенния заголовка при XSLT-преобразовании из сообщения: ").append(ConvXMLuseXSLTerr.toString());
                    // Считаем, что виноват клиент
                    return 5L;
                }

                try { // Парсим заголовок - получаем атрибуты messageQueueVO для сохранения в БД
                    XMLutils.Soap_HeaderRequest2messageQueueVO(Message.Soap_HeaderRequest.toString(), messageQueueVO, MessegeReceive_Log);

                }
                catch (Exception e) {
                    System.err.println( "Queue_Id["+ messageQueueVO.getQueue_Id() + "]  Exception: " + e.getMessage());
                    // e.printStackTrace();
                    MessegeReceive_Log.error(Queue_Direction + "fault: [" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: " + sStackTrace.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString() + ", fault: " + sStackTrace.strInterruptedException(e));

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString(),
                            e, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 7L;
                }

            }
            else { // Берем распарсенный Context из Header сообщения для сохранения в БД
                if ( Message.Input_Header_Context != null ) {
                    try {
                        XMLutils.Soap_XMLDocument2messageQueueVO(Message.Input_Header_Context, messageQueueVO, MessegeReceive_Log);
                    } catch (Exception e) {
                        System.err.println("Queue_Id [" + messageQueueVO.getQueue_Id() + "]  Exception");
                        e.printStackTrace();
                        MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTrace.strInterruptedException(e));
                        Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Queue_Direction + ", fault: " + sStackTrace.strInterruptedException(e));

                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                                "Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                                e, MessegeReceive_Log);
                        // Считаем, что виноват клиент
                        return 9L;

                    }
                }
                else {
                    MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_XMLDocument2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " );
                    Message.MsgReason.append("Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения (: ").append(Message.XML_MsgClear.toString()).append(")");

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
                    MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTrace.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Queue_Direction + ", fault: " + sStackTrace.strInterruptedException(e));

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
                                                             ConvXMLuseXSLTerr,  MessegeReceive_Log );

        }
        catch (Exception e) {
            System.err.println( "performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            e.printStackTrace();
            MessegeReceive_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            MessegeReceive_Log.error( "что то пошло совсем не так...");
            MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                    "performMessage Exception fault:"  + e.getMessage() + " " + Message.XML_MsgClear.toString()  ,
                    null ,  MessegeReceive_Log);
            //  создание Http-клиента перенеено в PerfotmInputMessagesюperformMessage()

        }

        return Function_Result.longValue(); // messageQueueVO.getQueue_Id();
    }


}
