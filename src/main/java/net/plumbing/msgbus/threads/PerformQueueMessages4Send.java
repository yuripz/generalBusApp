package net.plumbing.msgbus.threads;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import javax.xml.XMLConstants;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.*;

import java.nio.charset.StandardCharsets;
//import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.utils.*;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.xlstErrorListener;

// import static net.plumbing.msgbus.common.sStackTrace.strInterruptedException;

public class PerformQueueMessages4Send {

    private xlstErrorListener XSLTErrorListener=null;

    private String ConvXMLuseXSLTerr = "";

    //public void setExternalConnectionManager( ThreadSafeClientConnManager externalConnectionManager ) {this.ExternalConnectionManager = externalConnectionManager;}
    //public void setConvXMLuseXSLTerr( String p_ConvXMLuseXSLTerr) { this.ConvXMLuseXSLTerr = p_ConvXMLuseXSLTerr; }

    public  long performMessage(MessageDetails4Send Message, MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {
        // 1. Получаем шаблон обработки для MessageQueueVO
        String SubSys_Cod = messageQueueVO.getSubSys_Cod();
        int MsgDirection_Id = messageQueueVO.getMsgDirection_Id();
        int Operation_Id = messageQueueVO.getOperation_Id();
        Long Queue_Id = messageQueueVO.getQueue_Id();
        String Queue_Direction = messageQueueVO.getQueue_Direction();
        String AnswXSLTQueue_Direction=Queue_Direction;

        String URL_SOAP_Send = "";
        int Function_Result = 0;

        XSLTErrorListener = new xlstErrorListener();
        XSLTErrorListener.setXlstError_Log( MessageSend_Log );

        MessageSend_Log.info("{} [{}] ищем Шаблон под оперрацию ({}), с учетом системы приёмника MsgDirection_Id={}, SubSys_Cod ={}", Queue_Direction, Queue_Id, Operation_Id, MsgDirection_Id, SubSys_Cod);

        // ищем Шаблон под оперрацию, с учетом системы приёмника MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        int Template_Id = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(Operation_Id, MsgDirection_Id, SubSys_Cod, MessageSend_Log);
        MessageSend_Log.info("{} [{}]  Шаблон под оперрацию ={}", Queue_Direction, Queue_Id, Template_Id);

        if ( Template_Id < 0 ) {
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO , "Не нашли шаблон обработки сообщения для комбинации: Идентификатор системы[" + MsgDirection_Id + "] Код подсистемы[" + SubSys_Cod + "]", MessageSend_Log);
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()), monitoringQueueVO, MessageSend_Log);
            return -11L;
        }

        int messageTypeVO_Key = MessageRepositoryHelper.look4MessageTypeVO_2_Perform( Operation_Id, MessageSend_Log );
        if ( messageTypeVO_Key < 0  ) {
            MessageSend_Log.error("[{}] MessageRepositoryHelper.look4MessageTypeVO_2_Perform: Не нашли тип сообщения для Operation_Id=[{}]", Queue_Id, Operation_Id);
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]", MessageSend_Log);
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
            return -11L;
        }
        URL_SOAP_Send = MessageType.AllMessageType.get(messageTypeVO_Key).getURL_SOAP_Send();

        int MsgDirectionVO_Key = MessageRepositoryHelper.look4MessageDirectionsVO_2_Perform(MsgDirection_Id, SubSys_Cod, MessageSend_Log);

        if ( MsgDirectionVO_Key >= 0 )
            MessageSend_Log.info("[{}] MsgDirectionVO  getDb_pswd={}{}", Queue_Id, MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_pswd(), MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).LogMessageDirections());
        else {
            MessageSend_Log.error(Queue_Direction +" ["+ Queue_Id +"] Не нашли систему-приёмник для пары[" + MsgDirection_Id + "][" + SubSys_Cod + "]" );
            if ( theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли систему-приёмник для пары[" + MsgDirection_Id + "][" + SubSys_Cod + "]", MessageSend_Log) < 0 )
            {   // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
                return -11L;
            }
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
            return -12L;
        }

        Message.MessageTemplate4Perform = new MessageTemplate4Perform4Send (MessageTemplate.AllMessageTemplate.get(Template_Id),
                URL_SOAP_Send, //  хвост для добавления к getWSDL_Name() из MessageDirections
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getWSDL_Name(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_user(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_pswd(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getType_Connect(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getShort_retry_count(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getShort_retry_interval(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getLong_retry_count(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getLong_retry_interval(),
                Queue_Id,
                MessageSend_Log
        );
        MessageSend_Log.info("[{}] MessageTemplate4Perform[{}", Queue_Id, Message.MessageTemplate4Perform.printMessageTemplate4Perform());

        switch (Queue_Direction){
            case XMLchars.DirectOUT:
                // читаем их БД тело XML
                MessageSend_Log.info("{} [{}] зачитывем из БД тело XML, IsDebugged={}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getIsDebugged());
                MessageUtils.ReadMessageDetai4Send( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                if ( Message.MessageTemplate4Perform.getMessageXSD() != null )
                { boolean is_Message_OUT_Valid;
                    is_Message_OUT_Valid = TestXMLByXSD( Message.XML_MsgOUT.toString(), Message.MessageTemplate4Perform.getMessageXSD(), Message.MsgReason, MessageSend_Log );
                    if ( ! is_Message_OUT_Valid ) {
                        MessageSend_Log.error(" [{}] validateXMLSchema: message\n{}\n is not valid for XSD\n{}", Queue_Id, Message.XML_MsgOUT.toString(), Message.MessageTemplate4Perform.getMessageXSD());
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "validateXMLSchema: message {" + Message.XML_MsgOUT.toString() + "} is not valid for XSD {" + Message.MessageTemplate4Perform.getMessageXSD() + "}" ,
                                null ,  MessageSend_Log);
                        return -1L;
                    }
                }
                // преобразовываем тело
                String MessageXSLT_4_OUT_2_SEND = Message.MessageTemplate4Perform.getMessageXSLT();
                if ( MessageXSLT_4_OUT_2_SEND != null ) {
                    String XML_4_XSLT;
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("[{}] {} XSLT-преобразователь тела:{{}}",  Queue_Id, Queue_Direction, MessageXSLT_4_OUT_2_SEND);
                        // если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null ))
                    {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info(Queue_Direction + " [" + Queue_Id + "] SearchString:{" + Message.MessageTemplate4Perform.getPropSearchString() +"}, Replacement:{" + Message.MessageTemplate4Perform.getPropReplacement() +"}");
                        XML_4_XSLT = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else XML_4_XSLT = Message.XML_MsgOUT.toString();
                    try {
                        // Чисто для проверки конструкторов byte[] bb = new Message.XML_MsgOUT;Message.XML_MsgOUT.toString()
                        //StringBuilder xmlStringBuilder = new StringBuilder();
                        //ByteArrayInputStream xmlByteArrayInputStream  = new ByteArrayInputStream( xmlStringBuilder.toString().getBytes("UTF-8") );

                        Message.XML_MsgSEND = ConvXMLuseXSLT(Queue_Id, XML_4_XSLT, // Message.XML_MsgOUT.toString(),
                                MessageXSLT_4_OUT_2_SEND, Message.MsgReason,
                                MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged()
                        ).substring(XMLchars.xml_xml.length());// берем после <?xml version="1.0" encoding="UTF-8"?>
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info("{} [{}] после XSLT=:{{}}", Queue_Direction, Queue_Id, Message.XML_MsgSEND);
                    } catch ( TransformerException  exception ) {
                        MessageSend_Log.error("{} [{}] ConvXMLuseXSLT fault: {}", Queue_Direction, Queue_Id, exception.getMessage());
                        MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_OUT_2_SEND +"}");
                        MessageSend_Log.error("{} [{}] после XSLT=:{{}}", Queue_Direction, Queue_Id, Message.XML_MsgSEND);
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault: message=`" + ConvXMLuseXSLTerr + "` XSLT=`" + XML_4_XSLT+ "` on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessageSend_Log);
                        // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, XML_4_XSLT,  Message.XML_MsgOUT.toString(), monitoringQueueVO, MessageSend_Log);
                        return -2L;
                    }
                    if ( Message.XML_MsgSEND.equals(XMLchars.nanXSLT_Result) ) {
                        MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_OUT_2_SEND +"}");
                        MessageSend_Log.error("{} [{}] после XSLT=:`{}`", Queue_Direction, Queue_Id, Message.XML_MsgSEND);
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault message: " + ConvXMLuseXSLTerr + XML_4_XSLT + " on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessageSend_Log);
                        return -201L;
                    }

                     // сохраняем результат XSLT-преобразования( body ) распарсенный по-строчно <Tag><VALUE>
                    if ( MessageUtils.ReplaceMessage4SEND( theadDataAccess, Queue_Id, Message, messageQueueVO, MessageSend_Log)  < 0 )
                    { // Результат преобразования не получилось записать в БД
                        //HE-5864 Спец.символ UTF-16 или любой другой invalid XML character . Ошибка при отправке - удаляет и не записывант сообщение по
                       // Внутри ReplaceMessage4SEND вызов MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess, "ReplaceMessage4SEND fault" + Message.XML_MsgSEND  ,ex,  MessageSend_Log);
                        return -202L;
                    }
                }       //   MessageXSLT_4_OUT_2_SEND != null
                else
                {  // что на входе, то и отправляем, если нет MessageXSLT для преобразования
                    // ! но если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null )) {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info(Queue_Direction + " [" + Queue_Id + "] SearchString:{" + Message.MessageTemplate4Perform.getPropSearchString() +"}, Replacement:{" + Message.MessageTemplate4Perform.getPropReplacement() +"}");

                        Message.XML_MsgSEND = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else
                    Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                }

                // устанавливаем признак "SEND" & COMMIT
                if ( theadDataAccess.doUPDATE_MessageQueue_Out2Send( messageQueueVO, "XSLT (OUT) -> (SEND) ok",  MessageSend_Log) < 0 )
                {
                    return -203L;
                }


            case XMLchars.DirectSEND:
                if ( !Queue_Direction.equals("OUT") ) {
                    // надо читать из БД
                    MessageSend_Log.info("{}-> SEND [{}] читаем SEND БД тело XML", Queue_Direction, Queue_Id);
                    MessageUtils.ReadMessageDetai4Send( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                    if ( Message.MessageRowNum <= 0 ) {
                        MessageSend_Log.error("{}-> SEND [{}] тело XML для SEND в БД пустое !", Queue_Direction, Queue_Id);
                        MessageUtils.ProcessingOutError(  messageQueueVO,   Message,  theadDataAccess,
                                Queue_Direction +"-> SEND ["+ Queue_Id +"] тело XML для SEND в БД пустое !" ,
                                null ,  MessageSend_Log);
                        return -3L;
                    }


                    Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                    // Queue_Direction = "SEND";
                }
                Queue_Direction = XMLchars.DirectSEND;
                messageQueueVO.setQueue_Direction(XMLchars.DirectSEND);
                // if ( Queue_Direction.equalsIgnoreCase( "SEND") ) break; // ИЩЕМ Утечку потоков !!!

                // вызов внешней системы
                // провевяем , вдруг это REST
                MessageSend_Log.info("doSEND [{}] getPropWebMetod={} EndPointUrl={} PropTimeout_Conn={} PropTimeout_Read={} Type_Connection={} ShortRetryCount={} LongRetryCount={}",
                                      Queue_Id, Message.MessageTemplate4Perform.getPropWebMetod(), Message.MessageTemplate4Perform.getEndPointUrl(), Message.MessageTemplate4Perform.getPropTimeout_Conn(), Message.MessageTemplate4Perform.getPropTimeout_Read(), Message.MessageTemplate4Perform.getType_Connection(), Message.MessageTemplate4Perform.getShortRetryCount(), Message.MessageTemplate4Perform.getLongRetryCount());

                if ((Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) &&
                    (Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.JavaClassExeMetod)) )
                {
                  // 2.1) Это JDBC-обработчик. Используется для организации SQL запроса к "чужой" БД через дополнительный пулл
                  //----------------------------------------------------------------------------
                 // Тут JDBC-обработчик не поддерживается с Envelope4XSLTExt - надо орать!
                    MessageSend_Log.error("[{}] Тут JDBC-обработчик не поддерживается.В шаблоне для {} прописан Envelope4XSLTExt для XSLTExt-обработки", Queue_Id, Message.MessageTemplate4Perform.getTemplate_name());
                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT( messageQueueVO,
                                "JDBC-обработчик не поддерживается. В шаблоне для " + Message.MessageTemplate4Perform.getTemplate_name() + " прописан Envelope4XSLTExt для XSLTExt-обработки", 3233,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);
                        Message.MsgReason.append( "JDBC-обработчик не поддерживается. В шаблоне для " ).append( Message.MessageTemplate4Perform.getTemplate_name() ).append( " прописан Envelope4XSLTExt для XSLTExt-обработки");
                        return -35L;
                  //----------------------------------------------------------------------------  
                    
                }
                // Это может быть только SOAP или Rest
                // http : SOAP или Rest ( post| get)  если getPropWebMethod() != null
                        // готовим НАБОР заголовков HTTP на основе данных до XSLT преобразования OUT->SEND
                        Message.Soap_HeaderRequest.setLength(0);
                        if (Message.MessageTemplate4Perform.getHeaderXSLT() != null &&
                            Message.MessageTemplate4Perform.getHeaderXSLT().length() > 10) // Есть чем преобразовывать HeaderXSLT
                        {
                            if (Message.MessageTemplate4Perform.getIsDebugged()) {
                                MessageSend_Log.info("{} [{}] XSLT-преобразователь заголовка (чем):`{}`", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getHeaderXSLT());
                                MessageSend_Log.info("{} [{}] XSLT-преобразователь заголовка (что):`{}`", Queue_Direction, Queue_Id, Message.XML_MsgOUT);
                            }
                            try {
                                Message.Soap_HeaderRequest.append(
                                        ConvXMLuseXSLT(messageQueueVO.getQueue_Id(), Message.XML_MsgOUT.toString(), // содержание того, что отправляем
                                                Message.MessageTemplate4Perform.getHeaderXSLT(),  // через HeaderXSLT
                                                Message.MsgReason, MessageSend_Log,
                                                Message.MessageTemplate4Perform.getIsDebugged()
                                        )
                                                .substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                                );
                            } catch (TransformerException exception) {
                                MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь заголовка:{" + Message.MessageTemplate4Perform.getHeaderXSLT() + "}");

                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                        "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(), 1244,
                                        messageQueueVO.getRetry_Count(), MessageSend_Log);

                                return -5L;
                            }
                        }

                        if (Message.MessageTemplate4Perform.getPropWebMetod() != null) {
                            if (Message.MessageTemplate4Perform.getPropWebMetod().equals("get")) {
                                Function_Result = MessageHttpSend.HttpGetMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            }
                            if (Message.MessageTemplate4Perform.getPropWebMetod().equals("post")) {
                                String AckXSLT_4_make_JSON = Message.MessageTemplate4Perform.getAckXSLT() ; // получили XSLT-для
                                if ( AckXSLT_4_make_JSON != null ) {
                                    if (Message.MessageTemplate4Perform.getIsDebugged())
                                        MessageSend_Log.info("[{}] PropWebMetod is `post`, AckXSLT_4_make_JSON ({})", Queue_Id, AckXSLT_4_make_JSON);
                                    try {
                                        String make_JSON =
                                                ConvXMLuseXSLT(messageQueueVO.getQueue_Id(),
                                                        Message.XML_MsgSEND, // то, что подготовлено для передачи во внешнюю систему в формате XML
                                                        AckXSLT_4_make_JSON,  // через HeaderXSLT
                                                        Message.MsgReason, MessageSend_Log,
                                                        Message.MessageTemplate4Perform.getIsDebugged()
                                                );
                                        Message.XML_MsgSEND = make_JSON; // сохраняем для отправки результат преобразования
                                        if (Message.MessageTemplate4Perform.getIsDebugged())
                                            MessageSend_Log.info("[{}] PropWebMetod is `post`as JSON ({})", Queue_Id, Message.XML_MsgResponse);

                                    } catch (TransformerException exception) {
                                        MessageSend_Log.error("SEND [{}] XSLT-преобразователь для JSON :`{}`", messageQueueVO.getQueue_Id(), AckXSLT_4_make_JSON);

                                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                                "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + AckXSLT_4_make_JSON, 1244,
                                                messageQueueVO.getRetry_Count(), MessageSend_Log);

                                        System.err.println("[" + messageQueueVO.getQueue_Id() + "] TransformerException ");
                                        exception.printStackTrace();
                                        MessageSend_Log.error("[{}] from XML `{}` XSLT: `{}` JSON fault:{}", messageQueueVO.getQueue_Id(), Message.XML_MsgSEND, AckXSLT_4_make_JSON, exception.getMessage());
                                        Message.MsgReason.append(" XSLT-преобразователь для JSON `").append(AckXSLT_4_make_JSON).append("` fault: ").append(sStackTrace.strInterruptedException(exception));
                                        MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                                                "XSLT-преобразователь для JSON", true, exception, MessageSend_Log);
                                        return -402L;
                                    }
                                }

                                Function_Result = MessageHttpSend.sendPostMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            }
                            if ((!Message.MessageTemplate4Perform.getPropWebMetod().equals("get")) &&
                                    (!Message.MessageTemplate4Perform.getPropWebMetod().equals("post"))) {
                                MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                                        "Свойство WebMetod[" + Message.MessageTemplate4Perform.getPropWebMetod() + "], указаное в шаблоне не 'get' и не 'post'", true,
                                        null, MessageSend_Log);
                                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND, "Свойство WebMetod["+ Message.MessageTemplate4Perform.getPropWebMetod() + "], указаное в шаблоне не 'get' и не 'post'",  monitoringQueueVO, MessageSend_Log);
                                //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, null, null, monitoringQueueVO, MessageSend_Log);
                                return -401L;
                            }
                        } else { // сообщение будет отправлено через SOAP
                            // готовим SOAP-заголовок
                            Message.Soap_HeaderRequest.setLength(0);
                            if (Message.MessageTemplate4Perform.getHeaderXSLT() != null && Message.MessageTemplate4Perform.getHeaderXSLT().length() > 10) // Есть чем преобразовывать HeaderXSLT
                                try {
                                    Message.Soap_HeaderRequest.append(
                                            ConvXMLuseXSLT(messageQueueVO.getQueue_Id(), MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key), // стандартный заголовок c учетом системы-получателя
                                                    Message.MessageTemplate4Perform.getHeaderXSLT(),  // через HeaderXSLT
                                                    Message.MsgReason, MessageSend_Log,
                                                    Message.MessageTemplate4Perform.getIsDebugged()
                                            )
                                                    .substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                                    );
                                } catch (TransformerException exception) {
                                    MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь заголовка:{" + Message.MessageTemplate4Perform.getHeaderXSLT() + "}");

                                    theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                            "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(), 1244,
                                                        messageQueueVO.getRetry_Count(), MessageSend_Log);
                                    return -5L;
                                }
                            else
                                Message.Soap_HeaderRequest.append(MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key));
                            // Собсвенно, ВЫЗОВ!
                            Function_Result = MessageHttpSend.sendSoapMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            // MessageSend_Log.info("sendSOAPMessage:" + Queue_Direction + " [" + Queue_Id + "] для SOAP=:\n" + Message.XML_MsgSEND);
                        }

                if ( Function_Result <0 ) {
                    // TODO
                    // Надо бы всзести переменную - что c Http всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                     AnswXSLTQueue_Direction = messageQueueVO.getQueue_Direction();
                    break;
                }

                // шаблон MsgAnswXSLT заполнен
                if ( Message.MessageTemplate4Perform.getMsgAnswXSLT() != null) {
                    if ( Message.MessageTemplate4Perform.getIsDebugged()  ) {
                        MessageSend_Log.info(Queue_Direction + " [" + Queue_Id + "] MsgAnswXSLT: " + Message.MessageTemplate4Perform.getMsgAnswXSLT() );
                    }
                    try {
                    Message.XML_MsgRESOUT.append(
                            ConvXMLuseXSLT(
                                    Queue_Id, Message.XML_ClearBodyResponse.toString(), // очищенный от ns: /Envelope/Body
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT(),  // через MsgAnswXSLT
                                    Message.MsgReason, MessageSend_Log,
                                    Message.MessageTemplate4Perform.getIsDebugged()
                                    )
                                    .substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    } catch ( Exception exception ) {
                        MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь ответа:{" + Message.MessageTemplate4Perform.getMsgAnswXSLT() +"}");

                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, //.getQueue_Id(),
                                "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(), 1243,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);

                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO,Message.XML_ClearBodyResponse.toString(),
                        //        "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(),  monitoringQueueVO, MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                        return -501L;
                    }
                    // MessageSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] Message.MessageTemplate4Perform.getIsDebugged()=" + Message.MessageTemplate4Perform.getIsDebugged() );
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] преобразовали XML-ответ в: " + Message.XML_MsgRESOUT.toString() );
                }
                else // берем как есть без преобразования
                {
                    Message.XML_MsgRESOUT.append(Message.XML_ClearBodyResponse.toString());
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info(Queue_Direction + " [" + Queue_Id + "] используем XML-ответ как есть без преобразования:(" + Message.XML_MsgRESOUT.toString() + ")");
                }
                    // Проверяем наличие TagNext ="Next" в XML_MsgRESOUT
                AnswXSLTQueue_Direction = MessageUtils.PrepareConfirmation(  theadDataAccess,  messageQueueVO,  Message, MessageSend_Log );
                messageQueueVO.setQueue_Direction(AnswXSLTQueue_Direction);

                if ( !AnswXSLTQueue_Direction.equals(XMLchars.DirectRESOUT))
                    // TODO Надо бы всзести переменную - что c XSLT всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                {  // перечитываем состояние заголовка сообщения из БД
                    theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessageSend_Log );
                    break;
                }

                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
                messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_ClearBodyResponse.toString(), Message.XML_MsgRESOUT.toString(),  monitoringQueueVO, MessageSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);

                // получение и преобразование результатов
            case XMLchars.DirectRESOUT : //"RESOUT"
                // проверяем НАЛИЧИЕ пост-обработчика в Шаблоне
                if ( Message.MessageTemplate4Perform.getConfigPostExec() != null ) { // 1) ConfigPostExec
                    if ( !Queue_Direction.equals("SEND") ) {
                        // надо читать из БД
                        MessageSend_Log.error(Queue_Direction +"-> DELOUT/ATTOUT/ERROUT ["+ Queue_Id +"] читаем SEND БД тело XML" );
                        MessageUtils.ReadMessageDetai4Send( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                        Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                        Queue_Direction = XMLchars.DirectPOSTOUT;
                        MessageSend_Log.error("["+ Queue_Id +"] Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан.  " );
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан. Сделано от защиты зацикливания", 1232,
                                messageQueueVO.getRetry_Count(),  MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND,
                        //        "Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан. Сделано от защиты зацикливания",  monitoringQueueVO, MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                        return -11L;
                    }
                    // TODO Надо бы всзести переменную - что  пост-обработчик  всё же вызвался хоть раз и если ошибка, то больше не надо.
                    messageQueueVO.setQueue_Direction(XMLchars.DirectRESOUT);
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
                    { // 2.1) Это JDBC-обработчик
                        if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost() != null ) { // 2) EnvelopeXSLTPost
                            if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost().length() > 0 ) {
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessageSend_Log.info("["+ Queue_Id +"] Шаблон EnvelopeXSLTPost для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessageSend_Log.info("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost4Send( messageQueueVO,  Message, MessageSend_Log) );

                                String Passed_Envelope4XSLTPost;
                                try {
                                    Passed_Envelope4XSLTPost= ConvXMLuseXSLT(messageQueueVO.getQueue_Id(),
                                            MessageUtils.PrepareEnvelope4XSLTPost4Send( messageQueueVO, Message, MessageSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  // через EnvelopeXSLTPost
                                            Message.MsgReason, MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                                } catch ( TransformerException exception ) {
                                    MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-пост-преобразователь ответа:{" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() +"}");
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(), 1235,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessageSend_Log),
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  monitoringQueueVO, MessageSend_Log);
                                    return -101L;
                                }
                                if ( Passed_Envelope4XSLTPost.equals(XMLchars.EmptyXSLT_Result))
                                {   MessageSend_Log.error("["+ Queue_Id +"] Шаблон для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                    MessageSend_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost4Send(  messageQueueVO,  Message, MessageSend_Log) );
                                    MessageSend_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для пост-обработки " + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    return -12L;

                                }

                                final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML4Send( theadDataAccess,  Passed_Envelope4XSLTPost, messageQueueVO, Message,
                                                                                             Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log
                                                                                          );
                                if (resultSQL != 0) {
                                    MessageSend_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost4Send( messageQueueVO,  Message, MessageSend_Log) );
                                    MessageSend_Log.error("["+ Queue_Id +"] Ошибка ExecuteSQLinXML(" + resultSQL + "):" + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    return -13L;
                                }
                                else
                                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessageSend_Log.info("["+ Queue_Id +"] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString() );
                                    // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    /*if ( theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessageSend_Log ) == 0 )
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    else
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "do_SelectMESSAGE_QUEUE fault " ,  monitoringQueueVO, MessageSend_Log);
                                     */
                                }
                            }
                            else
                            {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                                MessageSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                                theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1232,
                                        messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                return -14L;
                            }
                        }
                        else
                        {
                            // Нет EnvelopeXSLTPost - надо орать!
                            MessageSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1232,
                                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
                            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                            //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost",  monitoringQueueVO, MessageSend_Log);
                            return -15L;
                        }
                    }
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
                    { // 2.2) Это Rest-HttpGet-вызов
                        // Нет параметров для Rest-HttpGet - надо орать!
                        MessageSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки указан неподдерживаемый для асинхронной обработки ответа метод " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() );
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "В шаблоне для пост-обработки указан неподдерживаемый для асинхронной обработки ответа метод " + Message.MessageTemplate4Perform.getPropExeMetodPostExec(), 1232,
                                messageQueueVO.getRetry_Count(),  MessageSend_Log);
                        return -16L;
                    }

                }
                else
                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info("["+ Queue_Id +"] ExeMetod для пост-обработки(" + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + ")");
                }
                // вызов пост-обработчика завершён

            case "ERROUT":
                // вызов пост-обработчика ??? - вызов при необходимости, ноавая фича

            case "DELOUT":
                break;
          default:
                break;
        }

        if ( Message.MessageTemplate4Perform.getIsDebugged() ) {
            MessageSend_Log.info("[" + Queue_Id + "] string 759:" );
            MessageSend_Log.info("[" + Queue_Id + "] AnswXSLTQueue_Direction='" + AnswXSLTQueue_Direction + "'");
            MessageSend_Log.info("[" + Queue_Id + "] messageQueueVO.getQueue_Direction()='" + messageQueueVO.getQueue_Direction() + "'");
        }

        if ( AnswXSLTQueue_Direction.equals(XMLchars.DirectERROUT)
        && !messageQueueVO.getQueue_Direction().equals(XMLchars.DirectRESOUT)) {
            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("["+ Queue_Id +"] ExeMetod для пост-обработки сообщения, получившего ошибку от внешней системы (" + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + ")");
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
            // вызов пост-обработчика ??? - вызов при необходимости, ноавая фича
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
            { // 2.2) Это Rest-HttpGet-вызов

                MessageSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки указан неподдерживаемый для асинхронной обработки ответа метод " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() );
                theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                        "В шаблоне для пост-обработки указан неподдерживаемый для асинхронной обработки ответа метод " + Message.MessageTemplate4Perform.getPropExeMetodPostExec(), 1232,
                        messageQueueVO.getRetry_Count(),  MessageSend_Log);

                return -161L;
            }
            //---------------------
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
            {
                if (Message.MessageTemplate4Perform.getErrTransXSLT() != null) { // 2) getErrTransXSLT
                    if (!Message.MessageTemplate4Perform.getErrTransXSLT().isEmpty()) {
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessageSend_Log.info("[" + Queue_Id + "] Шаблон ErrTransXSLT для пост-обработки(" + Message.MessageTemplate4Perform.getErrTransXSLT() + ")");
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessageSend_Log.info("[" + Queue_Id + "] ErrTransXSLT:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));

                        String Passed_Envelope4ErrTransXSLT;
                        try {
                            Passed_Envelope4ErrTransXSLT = ConvXMLuseXSLT(messageQueueVO.getQueue_Id(),
                                    MessageUtils.PrepareEnvelope4ErrTransXSLT( messageQueueVO, Message, MessageSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                    Message.MessageTemplate4Perform.getErrTransXSLT(),  // через getErrTransXSLT
                                    Message.MsgReason, MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                        } catch (TransformerException exception) {
                            MessageSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT для обработки ERROUT ответа:{" + Message.MessageTemplate4Perform.getErrTransXSLT() + "}");
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT" + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), 1295,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log),
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), monitoringQueueVO, MessageSend_Log);
                            return -18L;
                        }
                        if (Passed_Envelope4ErrTransXSLT.equals(XMLchars.EmptyXSLT_Result)) {
                            MessageSend_Log.error("[" + Queue_Id + "] Шаблон для обработки ERROUT(" + Message.MessageTemplate4Perform.getErrTransXSLT() + ")");
                            MessageSend_Log.error("[" + Queue_Id + "] Envelope4ErrTransXSLT:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));
                            MessageSend_Log.error("[" + Queue_Id + "] Ошибка преобразования XSLT для обработки ERROUT " + Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            return -19L;

                        }

                        final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML4Send(theadDataAccess, Passed_Envelope4ErrTransXSLT, messageQueueVO, Message,
                                                                                    Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log
                                                                                    );
                        if (resultSQL != 0) {
                            MessageSend_Log.error("[" + Queue_Id + "] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));
                            MessageSend_Log.error("[" + Queue_Id + "] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            return -20L;
                        } else {
                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessageSend_Log.info("[" + Queue_Id + "] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString());
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            /*if (theadDataAccess.do_SelectMESSAGE_QUEUE(messageQueueVO, MessageSend_Log) == 0)
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            else
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "do_SelectMESSAGE_QUEUE fault ", monitoringQueueVO, MessageSend_Log);
                            */
                        }
                    } else {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                        MessageSend_Log.error("[" + Queue_Id + "] В шаблоне для обработки ERROUT " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет ErrTransXSLT");
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет ErrTransXSLT", 1292,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);
                        return -21L;
                    }
                }
                else {
                    MessageSend_Log.warn("[" + Queue_Id + "] для обработки ERROUT " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " В шаблоне нет секции ErrTransXSLT");
                }
            }
            //-----------------------
        }

/*
        try {  SimpleHttpClient.close(); } catch ( IOException e) {
           MessageSend_Log.error("под конец  ошибка SimpleHttpClient.close(): " + e.getMessage() );
            Message.SimpleHttpClient = null;
            return messageQueueVO.getQueue_Id();      }
*/

        return messageQueueVO.getQueue_Id();
    }

   // private Security endpointProperties;
/*
    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }
*/
    private  boolean TestXMLByXSD(@NotNull String xmldata, @NotNull String xsddata, StringBuilder MsgResult,  Logger MessageSend_Log)// throws Exception
    {
        Validator valid=null;
        StreamSource reqwsdl=null, xsdss = null;
        Schema shm= null;

        try
        { reqwsdl = new StreamSource(new ByteArrayInputStream(xmldata.getBytes()));
            xsdss   = new StreamSource(new ByteArrayInputStream(xsddata.getBytes()));
            shm = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(xsdss);
            valid =shm.newValidator();
            valid.validate(reqwsdl);

        }
        catch ( Exception exp ) {
            MessageSend_Log.error("Exception: " + exp.getMessage());
            MsgResult.setLength(0);
            MsgResult.append( "TestXMLByXSD:"); MsgResult.append( sStackTrace.strInterruptedException(exp) );
            return false;}
        MessageSend_Log.info("validateXMLSchema message\n" + xmldata + "\n is VALID for XSD\n" + xsddata );
        return true;
    }


    private String ConvXMLuseXSLT(@NotNull Long QueueId, @NotNull String xmldata, @NotNull String XSLTdata, StringBuilder MsgResult, Logger MessageSend_Log, boolean IsDebugged )
            throws TransformerException
    { StreamSource source,srcXSLT;
        Transformer transformer;
        StreamResult result;
        ByteArrayInputStream xmlInputStream=null;
        //BufferedInputStream  _xmlInputStream;
        ByteArrayOutputStream fOut=new ByteArrayOutputStream();
        String res=XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr="";
        try {
            xmlInputStream  = new ByteArrayInputStream( xmldata.getBytes(StandardCharsets.UTF_8) );
        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessageSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:");  MsgResult.append( ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        if ( (XSLTdata == null) || ( XSLTdata.length() < XMLchars.EmptyXSLT_Result.length() )  ) {
            if ( IsDebugged )
                MessageSend_Log.info("["+ QueueId  + "] length XSLTdata 4 transform is null OR  < " + XMLchars.EmptyXSLT_Result.length() );
            return XMLchars.EmptyXSLT_Result ;
        }
        source = new StreamSource(xmlInputStream);
        try {
            srcXSLT = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes(StandardCharsets.UTF_8)));
        }
                catch ( Exception exp ) {
                ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                exp.printStackTrace();
                System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
                MessageSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
                MsgResult.setLength(0);
                MsgResult.append( "ConvXMLuseXSLT:");  MsgResult.append( ConvXMLuseXSLTerr );
                return XMLchars.EmptyXSLT_Result ;
            }
        result = new StreamResult(fOut);
        try
        {
            TransformerFactory XSLTransformerFactory = TransformerFactory.newInstance();
             XSLTransformerFactory.setErrorListener( XSLTErrorListener ); //!!!! java.lang.IllegalArgumentException: ErrorListener !!!
           /* XSLTransformerFactory.setErrorListener(new ErrorListener() {
                public void warning(TransformerException te) {
                    log.warn("Warning received while processing a stylesheet", te);
                }
                */
         // transformer = TransformerFactory.newInstance().newTransformer(srcXSLT);
            transformer = XSLTransformerFactory.newTransformer(srcXSLT);
            if ( transformer != null) {
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                transformer.transform(source, result);
            }
            else result = null;

            if ( result != null) {
                res = fOut.toString();
                // System.err.println("result != null, res:" + res );
                if (( res.charAt(0) == '{') || ( res.charAt(0) == '['))
                {
                    if ( IsDebugged )
                        MessageSend_Log.warn("["+ QueueId  + "] json transformer.transform(`"+ res + "`) < "  );
                }

                 else
                if ( res.length() < XMLchars.EmptyXSLT_Result.length()) {
                    if ( IsDebugged )
                        MessageSend_Log.warn("["+ QueueId  + "] length transformer.transform(`"+ res + "`) < {}" , XMLchars.EmptyXSLT_Result.length() );
                    res = XMLchars.EmptyXSLT_Result;
                }
            }
            else {
                if ( IsDebugged )
                    MessageSend_Log.warn("["+ QueueId  + "] StreamResult transformer.transform() is null ");
                res = XMLchars.EmptyXSLT_Result;
                // System.err.println("result= null, res:" + res );
            }
        try { fOut.close();}
           catch( IOException IOexc)  {
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer IOException" );
            IOexc.printStackTrace(); }
            /*
                if ( IsDebugged ) {
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + xmldata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + res);
            }
            */
        }
        catch ( TransformerException exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer TransformerException" );
            exp.printStackTrace();
            MessageSend_Log.error("["+ QueueId  + "] ConvXMLuseXSLT.Transformer TransformerException: {}" , ConvXMLuseXSLTerr);
            if (  !IsDebugged ) {
                MessageSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML IN ): " + xmldata);
                MessageSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessageSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML out ): " + res);
            }
            MessageSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:");  MsgResult.append( ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(res);
    }

}
