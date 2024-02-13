package net.plumbing.msgbus.controller;
import com.google.common.io.CharStreams;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import org.apache.commons.io.Charsets;
import org.apache.commons.text.TextStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.slf4j.Marker;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
//import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
//import org.springframework.web.context.request.async.DeferredResult;
//import ServletApplication;
import net.plumbing.msgbus.common.ApplicationProperties;

import net.plumbing.msgbus.common.ClientIpHelper;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageTemplate;
//import TheadDataAccess;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;

import javax.servlet.ServletRequest;
import java.io.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
//import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

//import static javax.xml.soap.SOAPConstants.SOAP_1_1_CONTENT_TYPE;
import static net.plumbing.msgbus.common.XMLchars.*;
import static net.plumbing.msgbus.common.sStackTrace.strInterruptedException;
import static net.plumbing.msgbus.common.ApplicationProperties.DataSourcePoolMetadata;
import static net.plumbing.msgbus.model.MessageTemplateVO.*;

// @Controller
@RestController
public class PostController {

    private  static final Logger Controller_log = LoggerFactory.getLogger(PostController.class);

    @PostMapping(path = {"/MsgBusService/PostHttpRequest/**", "/MsgBusService/SoapRequest/**","/HermesService/PostHttpRequest/**", "/HermesService/SoapRequest/**", "/HermesSOAPService/SOAPServlet/**"}, produces = MediaType.ALL_VALUE, consumes = MediaType.ALL_VALUE)

    //   @ResponseStatus(HttpStatus.OK) // MediaType.TEXT_XML_VALUE.APPLICATION_XML_VALUE

    public @ResponseBody
    byte[] PostHttpRequest(ServletRequest postServletRequest, HttpServletResponse postResponse) {

        InputStream inputStream = null;
        //String Response = "ok";
        //Long timeOutInMilliSec = 100000L;
        //String timeOutResp = "Time Out.";
        //Charset charSet = null;
        HttpServletRequest httpRequest = (HttpServletRequest) postServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String soapAction;
        soapAction = httpRequest.getHeader("soapAction");
        if (soapAction == null) {
            if (url.indexOf("SoapRequest") > 0)
                soapAction = "SoapRequest";
        }
        Controller_log.info("PostHttpRequest: from \"" + ClientIpHelper.getClientIp(httpRequest) + "\" url= (" + url + ") soapAction:" + soapAction + " Real-IP:" + ClientIpHelper.getRealIP(httpRequest));

        String Url_Soap_Send = ClientIpHelper.findUrl_Soap_Send(url);

        Integer Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);

        if (Interface_id < 0) {
            postResponse.setStatus(404);
            String OutResponse;
            if (soapAction != null)
                OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                    httpRequest.getMethod() + ": Интерфейс для обработки (" + Url_Soap_Send + ") в системе не сконфигурирован" +
                    Fault_End + Body_End + Envelope_End;
            else OutResponse =  Fault_Client_Begin +
                    httpRequest.getMethod() + ": Интерфейс для обработки (" + Url_Soap_Send + ") в системе не сконфигурирован" +
                     Envelope_End;
            return OutResponse.getBytes();
        }

        try {
            inputStream = postServletRequest.getInputStream();
        } catch (IOException ioException) {
            postResponse.setStatus(500);
            ioException.printStackTrace(System.err);
            String OutResponse;
            if (soapAction != null)
                OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                    "postServletRequest.getInputStream fault:" + ioException.getMessage() +
                    Fault_End + Body_End + Envelope_End;
            else OutResponse =  Fault_Client_Begin +
                    "postServletRequest.getInputStream fault:" + ioException.getMessage() +
                    Fault_End;
            return OutResponse.getBytes();
        }

        // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
        int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
        String PropEncoding_Out = "UTF-8";
        String PropEncoding_In = "UTF-8";
       //  Controller_log.info("MessageTemplateVOkey:=" + MessageTemplateVOkey+ " , PostHttpRequest: CharacterEncoding=" + postServletRequest.getCharacterEncoding() );
        boolean isDebugged = false;
        String  PropCustomFault_Server_Begin=null; // Fault_Server_noNS_Begin;
        String  PropCustomFault_Server_End=null; //Fault_noNS_End;

        if (MessageTemplateVOkey >= 0)
        {
            String ConfigExecute = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getConfigExecute();
            // Controller_log.info("ConfigExecute:" + ConfigExecute);
            if (ConfigExecute != null) {
                Properties properties = new Properties();
                InputStream propertiesStream = new ByteArrayInputStream(ConfigExecute.getBytes(StandardCharsets.UTF_8));
                try {
                    properties.load(propertiesStream);
                    for (String key : properties.stringPropertyNames()) {
                        if (key.equals(PropNameCharOut)) PropEncoding_Out = properties.getProperty(key);
                        Charsets.toCharset(PropEncoding_Out);
                        if (key.equals(PropNameCharIn)) PropEncoding_In = properties.getProperty(key);
                        if (key.equals(PropCustomFaultBegin)) { // Для методов Post не интерфейсе может быть установлен отличный от XMLchars.Fault_Server_noNS_Begin -
                            PropCustomFault_Server_Begin = properties.getProperty(key); //  и XMLchars.Fault_noNS_End обрамление для описания ошибки
                        }
                        if (key.equals(PropCustomFaultEnd)) {
                            PropCustomFault_Server_End = properties.getProperty(key);
                        }
                        // Controller_log.info("Property[" + key + "]=[" + properties.getProperty(key) + "]");
                        if (key.equals(PropDebug)) {
                            // Controller_log.info("PropDebug Property[" + key + "]=[" + properties.getProperty(key) + "]");
                            if ((properties.getProperty(key).equalsIgnoreCase("on")) ||
                                    (properties.getProperty(key).equalsIgnoreCase("full"))
                            ) {
                                isDebugged = true;
                            }
                            if ((properties.getProperty(key).equalsIgnoreCase("ON")) ||
                                    (properties.getProperty(key).equalsIgnoreCase("FULL"))
                            ) {
                                isDebugged = true;
                            }
                        }
                    }
                } catch (IOException ioException) {
                    postResponse.setStatus(500);
                    ioException.printStackTrace(System.err);
                    Controller_log.error("properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage());
                    String OutResponse;
                    if (soapAction != null)
                        OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                            "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                            Fault_End + Body_End + Envelope_End;
                    else {
                        if ((PropCustomFault_Server_Begin != null ) && (PropCustomFault_Server_End !=null )) {
                            OutResponse = PropCustomFault_Server_Begin +
                                    "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                                    PropCustomFault_Server_End;
                        }
                        else
                        OutResponse = Fault_Client_Begin +
                                "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                                Fault_End;
                    }
                    return OutResponse.getBytes();
                }
            }
        }
        // else - нет шаблона, ну он и не всегда нужен

// InputStreamReader(inputStream, "Windows-1251"));

        MessageDetails Message = new MessageDetails();
        //PropEncoding_In = "UTF-8";
        try (InputStreamReader reader = new InputStreamReader(inputStream, Charsets.toCharset(PropEncoding_In))// Charsets.UTF_8)
        ) {
            if ( isDebugged ) {
                Controller_log.warn("Message.soapAction[" + soapAction + "]");
            }
            if (soapAction != null)
                Message.XML_MsgInput = CharStreams.toString(reader);
            else {
                int xmlVersionEncoding_pos = 0;
                /*Controller_log.error("----------------------------");
                Controller_log.error(CharStreams.toString(reader));
                Controller_log.error("----------------------------");
                 */
                Message.XML_MsgConfirmation.append(CharStreams.toString(reader));
                if ( isDebugged ) {
                    Controller_log.warn("Message.XML_MsgConfirmation.substring(0, 2)[" + Message.XML_MsgConfirmation.substring(0, 2) + "] Message.XML_MsgConfirmation.indexOf(\"?>\") =" + Message.XML_MsgConfirmation.indexOf("?>"));

                }
                if (Message.XML_MsgConfirmation.substring(0, 2).equals("<?")) {
                    // в запросе <?xml version="1.0" encoding="UTF-8"?> Ищем '?>' что бы изъять !
                    xmlVersionEncoding_pos = Message.XML_MsgConfirmation.indexOf("?>") + 2;
                }
                Message.XML_MsgInput = Envelope_noNS_Begin
                        + Header_noNS_Begin + Header_noNS_End
                        + Body_noNS_Begin
                        + Message.XML_MsgConfirmation.substring(xmlVersionEncoding_pos)// CharStreams.toString(reader)
                        + Body_noNS_End + Envelope_noNS_End
                ;
                if ( isDebugged )
                    Controller_log.warn("Message.XML_MsgConfirmation.substring("+xmlVersionEncoding_pos + ") [" +  Message.XML_MsgConfirmation.substring(xmlVersionEncoding_pos) + "]");
                 }
            if ( isDebugged )
            Controller_log.warn("InputStreamReader to Message.XML_MsgInput[" +  Message.XML_MsgInput + "]");
            inputStream.close();
        } catch (IOException ioException) {
            postResponse.setStatus(500);
            Controller_log.error("CharStreams.toString(getInputStream) fault:" + ioException.getMessage());
            if (soapAction != null) {
                String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                        "CharStreams.toString(getInputStream) fault:" + ioException.getMessage() +
                        Fault_End + Body_End + Envelope_End;
                return OutResponse.getBytes();
            } else {
                String OutResponse = Fault_Server_noNS_Begin + "CharStreams.toString(getInputStream) fault:" + ioException.getMessage()
                        + Fault_noNS_End;
                return OutResponse.getBytes();
            }
        }
        // очищаем использованный XML_MsgConfirmation
        Message.XML_MsgConfirmation.setLength(0);
        Message.XML_MsgConfirmation.trimToSize();


        MessageReceiveTask messageReceiveTask = new MessageReceiveTask();// (MessageSendTask) context.getBean("MessageSendTask");
    try
    {
// TODO ! isDebugged надо брать из PropDebug, но для этого у интерфейса должен быть шаблон
        //isDebugged = false; // для локальной отладки
    Long Queue_ID = messageReceiveTask.ProcessInputMessage(Interface_id, Message, MessageTemplateVOkey, isDebugged);

    // Controller_log.info("SOAP_1_1_CONTENT_TYPE=" + SOAP_1_1_CONTENT_TYPE );

        if (Queue_ID == 0L) {
            postResponse.setStatus(200);
            if (soapAction != null) // это SOAP
            {
                postResponse.setContentType("text/xml; charset=utf-8");
                String OutResponse = Envelope_Begin + Empty_Header + Body_Begin +
                        Message.XML_MsgResponse.toString() +
                        Body_End + Envelope_End;
                if ( messageReceiveTask.theadDataAccess != null)
                {
                    if (isDebugged) {
                        Controller_log.warn("OutResponse:[" + OutResponse + "]" );
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log);
                    }
                    try {
                        if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                    }
                }
                Controller_log.info("DataSourcePool " + ApplicationProperties.DataSourcePoolMetadata.getActive());
                return OutResponse.getBytes();
            } else {  // это ЛИРА или другой XML over Http-POST ,  например О20:(
                postResponse.setContentType("text/xml; charset=" + PropEncoding_Out);
                try {
                    // очищаем использованный XML_MsgConfirmation
                    Message.XML_MsgConfirmation.setLength(0);
                    Message.XML_MsgConfirmation.trimToSize();
                    byte[] OutResponse = Message.XML_MsgResponse.toString().getBytes(PropEncoding_Out);
                    Message.XML_MsgConfirmation.append(new String(OutResponse));
                    if (isDebugged)
                        Controller_log.warn("XML_MsgResponse Encoding  (" + PropEncoding_Out + "):" + Message.XML_MsgConfirmation);
                    if ( messageReceiveTask.theadDataAccess != null) {
                        if (isDebugged)
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, Message.XML_MsgResponse.toString(), Controller_log);
                        try {
                            if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                            messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                        } catch (SQLException SQLe) {
                            Controller_log.error(SQLe.getMessage());
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            SQLe.printStackTrace();
                        }
                    }
                    Controller_log.info("Post DataSourcePool " + DataSourcePoolMetadata.getActive());
                    return OutResponse;
                    // Message.XML_MsgConfirmation.append(Message.XML_MsgResponse.toString().getBytes(PropEncoding_Out) );
                } catch (UnsupportedEncodingException e) {
                    System.err.println("[ XML_MsgResponse Encoding:" + PropEncoding_Out + "] UnsupportedEncodingException");
                    e.printStackTrace();
                    Controller_log.error("XML_MsgResponse Encoding fault (" + PropEncoding_Out + ") UnsupportedEncodingException:" + e.getMessage());
                    String OutResponse = Fault_Server_noNS_Begin  + "XML_MsgResponse Encoding fault (" + PropEncoding_Out + ") UnsupportedEncodingException:" + e.getMessage() +
                                         Fault_noNS_End;
                    if ( messageReceiveTask.theadDataAccess != null)
                    {
                        if (isDebugged)
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log);
                        try {
                            if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                            messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                        } catch (SQLException SQLe) {
                            Controller_log.error(SQLe.getMessage());
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            SQLe.printStackTrace();
                        }
                    }
                    Controller_log.info("Post DataSourcePool " + DataSourcePoolMetadata.getActive());
                    return OutResponse.getBytes();
                }
            }
        } else {
            postResponse.setStatus(500);
            if (soapAction != null) // это SOAP
            {
                if (Queue_ID > 0L) {
                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                            XML.escape(Message.MsgReason.toString()) +
                            Fault_End + Body_End + Envelope_End;
                    if (messageReceiveTask.theadDataAccess != null) { // SOAP был был распознан
                        if (isDebugged)
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log);
                        try {
                            if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                            messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                        } catch (SQLException SQLe) {
                            Controller_log.error(SQLe.getMessage());
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            SQLe.printStackTrace();
                        }
                    }
                    Controller_log.info("Post DataSourcePool " + DataSourcePoolMetadata.getActive());
                    return OutResponse.getBytes();

                } else {
                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Server_Begin +
                            XML.escape(Message.MsgReason.toString()) +
                            Fault_End + Body_End + Envelope_End;
                    if ( messageReceiveTask.theadDataAccess != null)
                    {
                    if (isDebugged)
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log);
                        try {
                            if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                            messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                        } catch (SQLException SQLe) {
                            Controller_log.error(SQLe.getMessage());
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            SQLe.printStackTrace();
                        }
                    }
                    Controller_log.info("Post DataSourcePool " + DataSourcePoolMetadata.getActive());
                    return OutResponse.getBytes();
                }
            } else {  // это ЛИРА : может быть использовано PropCustomFault_Server_Begin
                String OutResponse;
                if ((PropCustomFault_Server_Begin != null ) && (PropCustomFault_Server_End !=null )) {
                    OutResponse = PropCustomFault_Server_Begin + XML.escape(Message.MsgReason.toString()) + PropCustomFault_Server_End;
                }
                else {
                    if (Queue_ID > 0L) {
                        OutResponse = Fault_Client_noNS_Begin + XML.escape(Message.MsgReason.toString()) + Fault_noNS_End;
                    }
                    else {
                        OutResponse = Fault_Server_noNS_Begin + XML.escape(Message.MsgReason.toString()) + Fault_noNS_End;
                    }

                }



                if ( messageReceiveTask.theadDataAccess != null) {
                    if (isDebugged)
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log);
                    try {
                        if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                    }
                }
                Controller_log.info("Post DataSourcePool " + DataSourcePoolMetadata.getActive());
                try  {
                return OutResponse.getBytes(PropEncoding_Out);
            } catch (UnsupportedEncodingException e) {
                    System.err.println("[ XML_MsgResponse Encoding:" + PropEncoding_Out + "] UnsupportedEncodingException");
                    e.printStackTrace();
                    Controller_log.error("XML_MsgResponse Encoding fault (" + PropEncoding_Out + ") UnsupportedEncodingException:" + e.getMessage());
                    return OutResponse.getBytes();
                }
            }
        }
    } finally {
            if (messageReceiveTask.theadDataAccess != null) {
                try {
                    if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                        messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                } catch (SQLException SQLe) {
                    Controller_log.error("finally: Hermes_Connection.close() fault:" + SQLe.getMessage());
                    SQLe.printStackTrace();

                }
            }

    }


    }

    @PostMapping(path = {"/MsgBusService/InternalRestApi/**","/HermesService/InternalRestApi/**"}, produces = MediaType.ALL_VALUE, consumes = MediaType.ALL_VALUE)
    // @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public
    String PostHermesRestApi(ServletRequest postServletRequest, HttpServletResponse postResponse)
    {
        InputStream inputStream = null;
        HttpServletRequest httpRequest = (HttpServletRequest) postServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String OperationId = httpRequest.getHeader("BusOperationId");
        String BusOperationMesssageType=null;
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), StandardCharsets.UTF_8);
        } catch (NullPointerException e) {
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("BusOperationId= " + OperationId );
        postResponse.setHeader("Access-Control-Allow-Origin", "*");

        Controller_log.info("PostHermesRestApi: from \"" + ClientIpHelper.getClientIp(httpRequest) + "\" url= (" + url + ") BusOperationId:" + OperationId + " Real-IP:" + ClientIpHelper.getRealIP(httpRequest));
        String HttpResponse= Fault_Client_Rest_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ")") +
                Fault_Rest_End;

        String Url_Soap_Send = ClientIpHelper.findTypes_URL_SOAP_SEND(url, "InternalRestApi/", Controller_log);
        if ( Url_Soap_Send == null)
        {   postResponse.setStatus(422);
            HttpResponse= Fault_Client_Rest_Begin +
                    "Клиент не передал в " +
                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")  интерфейс для обработки" ) +
                    Fault_Rest_End ;
            postResponse.setContentType("text/json;Charset=UTF-8");
            return HttpResponse;
        }

        Integer Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        if ( Interface_id < 0 )
        {   postResponse.setStatus(500);
            HttpResponse= Fault_Client_Rest_Begin +
                    "Ресурса нет на сервере: Интерфейс для обработки " +
                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    // + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")"
                    " в системе не сконфигурирован" +
                    Fault_Rest_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            postResponse.setContentType("text/json;Charset=UTF-8");
            return HttpResponse;
        }

        try {
            inputStream = postServletRequest.getInputStream();
        } catch (IOException ioException) {
            postResponse.setStatus(500);
            ioException.printStackTrace(System.err);
            String OutResponse = Fault_Client_Rest_Begin +
                    "postServletRequest.getInputStream fault:" + ioException.getMessage() +
                    Fault_Rest_End;
            postResponse.setContentType("text/json;Charset=UTF-8");
            return OutResponse;
        }
        // int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
        boolean isDebugged = true;
        // Начинае подготовку к обработке запроса
        if (OperationId == null )
        {
            BusOperationMesssageType = ClientIpHelper.find_BusOperationMesssageType(url, "InternalRestApi/" + Url_Soap_Send + "/", Controller_log);
            if (BusOperationMesssageType == null) {
                postResponse.setStatus(422);
                HttpResponse = Fault_Client_Rest_Begin +
                        "Клиент не передал в " +
                        org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") имя сущности (тип операции) для обработки") +
                        Fault_Rest_End;
                postResponse.setContentType("text/json;Charset=UTF-8");
                return HttpResponse;
            }
            Controller_log.warn("BusOperationMesssageType: [" + BusOperationMesssageType + "]");
            // формируем НАСТОЯЩИЙ тип операции из полученнго от URL + в зависимости от  queryString

//                if ( queryString == null) // Значит, в "InternalRestApi/"+ Url_Soap_Send +"/" может  быть ПК для зачитывания записи для GetOne
//                {
//                    // проверяем, есть ли ПК для зачитывания записи
//                    String EntityPK = ClientIpHelper.find_BusOperationMesssageType(url, "InternalRestApi/"+Url_Soap_Send+"/" + BusOperationMesssageType+"/" , Controller_log);
//                    Controller_log.warn("EntityPK: [" + EntityPK +"]");
//                    if (EntityPK != null ) { //  получен ПК для зачитывания записи
//                        OperationId = MessageRepositoryHelper.look4MessageTypeVO_by_MesssageType(BusOperationMesssageType + "GetOne", Interface_id, Controller_log);
//                        if (OperationId == null) {
//                            HttpResponse = Fault_Client_Rest_Begin +
//                                    "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType + "GetOne" + " для обработки " +
//                                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
//                                    " в системе не сконфигурирована" +
//                                    Fault_Rest_End;
//                            Controller_log.warn("HttpResponse:" + HttpResponse);
//                            getResponse.setContentType("text/json;Charset=UTF-8");
//                            getResponse.setStatus(422);
//                            return HttpResponse;
//                        } else {
//                            queryString = "Id=" + EntityPK;
//                        }
//                    }
//                    else { // ПК для зачитывания записи в URL не прислали - значит готовим список всего без фильтров и сортировки
//                        OperationId = MessageRepositoryHelper.look4MessageTypeVO_by_MesssageType(BusOperationMesssageType + "GetList", Interface_id, Controller_log);
//                        if (OperationId == null) {
//                            HttpResponse = Fault_Client_Rest_Begin +
//                                    "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType + "GetList" + " для обработки " +
//                                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
//                                    " в системе не сконфигурирована" +
//                                    Fault_Rest_End;
//                            Controller_log.warn("HttpResponse:" + HttpResponse);
//                            getResponse.setContentType("text/json;Charset=UTF-8");
//                            getResponse.setStatus(422);
//                            return HttpResponse;
//                        }
//                        // формируем дефалтовую  queryString что бы не упасть на разборе параметров, которых нет
//                        queryString = "page[number]=1&page[size]=100&sort=id";
//                        // TODO: 01.11.2020   в дальнейшем можно /нужно это брать их типа операции, есть "свободное" под это поле URL_SOAP_SEND:
//                    }
//
//                }
//                else // считаем, что пригнали параметры для получения списка записей GetList
//                {
//                    OperationId = MessageRepositoryHelper.look4MessageTypeVO_by_MesssageType(BusOperationMesssageType + "GetList", Interface_id, Controller_log);
//                    if (OperationId == null) {
//                        HttpResponse = Fault_Client_Rest_Begin +
//                                "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType + "GetList" + " для обработки " +
//                                org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
//                                " в системе не сконфигурирована" +
//                                Fault_Rest_End;
//                        Controller_log.warn("HttpResponse:" + HttpResponse);
//                        getResponse.setContentType("text/json;Charset=UTF-8");
//                        getResponse.setStatus(422);
//                        return HttpResponse;
//                    }
//                    Controller_log.warn("Орерация с типом:" + BusOperationMesssageType + "GetList" + " NN=" + OperationId);
//                }

            OperationId = MessageRepositoryHelper.look4MessageTypeVO_by_MesssageType(BusOperationMesssageType, Interface_id, Controller_log);
        }

        if (OperationId == null) {
            HttpResponse = Fault_Client_Rest_Begin +
                    "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType + " Post/Put" + " для обработки " +
                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
                    " в системе не сконфигурирована" +
                    Fault_Rest_End;
            Controller_log.warn("HttpResponse:" + HttpResponse);
            postResponse.setContentType("text/json;Charset=UTF-8");
            postResponse.setStatus(422);
            return HttpResponse;
        }
        Controller_log.warn("Операция с типом:" + BusOperationMesssageType + " NN=" + OperationId);
        Controller_log.warn("POST QueryString: [" + queryString + "]");
        MessageDetails Message = new MessageDetails();
        Message.XML_Request_Method.append(QueryString_Begin);
        if (queryString !=null)
        {
            String queryParams[];
            queryParams = queryString.split("&");
            // filter={}&range=[0,9]&sort=["id","ASC"])
            for (int queryParamIndex = 0; queryParamIndex < queryParams.length; queryParamIndex++) { // Controller_log.warn( queryParams[i]);
                String ParamElements[] = queryParams[queryParamIndex].split("=");
                // Controller_log.warn(ParamElements[0]);
                //String ParamElementName = ClientIpHelper.toCamelCase(ParamElements[0], "_");
                //int ParamElementNameLength = (ParamElementName.indexOf(']') > 0) ? ParamElementName.indexOf(']') : ParamElementName.length();
                try { // ?_end=5&_order=DESC&_sort=username&_start=0
                        // Controller_log.warn(ParamElements[0]);

                        Message.XML_Request_Method.append(OpenTag);
                        Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                        Message.XML_Request_Method.append(CloseTag);

                        if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                            Controller_log.warn(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                            Message.XML_Request_Method.append(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                        }

                        Message.XML_Request_Method.append(OpenTag);
                        Message.XML_Request_Method.append(EndTag);
                        Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                        Message.XML_Request_Method.append(CloseTag);
                }
                catch (StringIndexOutOfBoundsException | NumberFormatException e) {
                    // org.apache.commons.text.StringEscapeUtils.escapeJson();
                    // JSONObject json = new JSONObject("");
                    HttpResponse= Fault_Client_Rest_Begin + org.apache.commons.text.StringEscapeUtils.escapeJson(
                            "Ошибка при разборе параметров от клиента (" + queryString + ") " + e.getMessage() ) +
                            Fault_Rest_End ;
                    Controller_log.warn("HttpResponse:\n" + HttpResponse);
                    postResponse.setContentType("text/json;Charset=UTF-8");
                    postResponse.setStatus(422);
                    return HttpResponse;
                }
            }
        }
        Message.XML_Request_Method.append(QueryString_End);

        //PropEncoding_In = "UTF-8";
        try (InputStreamReader reader = new InputStreamReader(inputStream, Charsets.toCharset ("UTF-8")) ;// Charsets.UTF_8)
        ) {
                 Message.XML_MsgConfirmation.append( CharStreams.toString(reader) );
            if ( !MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface(Url_Soap_Send, Controller_log))
            {       // если на интерфейсе НЕ!! прописан REST-XML , то берем XML на входе
                postResponse.setContentType("text/xml;charset=UTF-8");

            if ( isDebugged )
                Controller_log.warn("InputStreamReader to Message.JSONObject[" +  Message.XML_MsgConfirmation.toString() + "]");

                JSONObject postJSONObject = new JSONObject( Message.XML_MsgConfirmation.toString() );
                Message.XML_MsgConfirmation.setLength(0);
                Message.XML_MsgConfirmation.trimToSize();
            Message.XML_MsgConfirmation.append( XML.toString( postJSONObject  , "Record" ) );
            }
            else {
                if ( isDebugged )
                    Controller_log.warn("InputStreamReader to XML_MsgConfirmation[" +  Message.XML_MsgConfirmation.toString() + "]");
            }
            Message.XML_MsgInput = Envelope_noNS_Begin
                    // + EmptyHeader
                    + Header_4BusOperationId_Begin + OperationId + Header_4BusOperationId_End
                    + Body_noNS_Begin + Parametrs_Begin
                    + Message.XML_MsgConfirmation.toString() + Message.XML_Request_Method.toString()
                    + Parametrs_End + Body_noNS_End + Envelope_noNS_End
            ;


            if ( isDebugged )
                Controller_log.warn("InputStreamReader to Message.XML_MsgInput[" +  Message.XML_MsgInput + "]");
            inputStream.close();
            // throw  new IOException( " Проверка ioException ! " ) ;
        } catch (IOException | JSONException|   IllegalCharsetNameException ioException ) {
            ioException.printStackTrace();
            postResponse.setStatus(500);
            Controller_log.error("CharStreams.toString(" +  Message.XML_MsgConfirmation.toString() + ") fault:" + ioException.getMessage());

                String OutResponse = Fault_Server_Rest_Begin + "CharStreams.toString(getInputStream) fault:" + ioException.getMessage()
                        + Fault_Rest_End;
                postResponse.setContentType("text/json;Charset=UTF-8");
                return OutResponse;

        }
        // очищаем использованный XML_MsgConfirmation
        Message.XML_MsgConfirmation.setLength(0);
        Message.XML_MsgConfirmation.trimToSize();

        RestAPI_ReceiveTask messageReceiveTask=null;
        try {
            messageReceiveTask = new RestAPI_ReceiveTask();
            // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
            int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
            int MessageOperationId = Integer.parseInt(OperationId);

            Long Queue_ID = messageReceiveTask.ProcessRestAPIMessage(Interface_id, Message, MessageOperationId, isDebugged);

            if (Queue_ID == 0L)
            {
                postResponse.setStatus(200);
                if ( MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(Url_Soap_Send, Controller_log))
                    // в URL_SOAP_Ack интерфейса записан REST, значит без <Body></Body>
                    HttpResponse = Message.XML_MsgResponse.toString();
                else HttpResponse = Body_noNS_Begin +
                        Message.XML_MsgResponse.toString() +
                        Body_noNS_End;
            } else {
                if (Queue_ID > 0L) {
                    postResponse.setStatus(500);
                    HttpResponse = Fault_Client_noNS_Begin +
                            XML.escape(Message.MsgReason.toString()) +
                            Fault_noNS_End;
                } else {
                    postResponse.setStatus(500);
                    HttpResponse = Fault_Server_noNS_Begin +
                            Message.MsgReason.toString() +
                            Fault_noNS_End;
                }
            }
           // postResponse.setStatus(200);
/*
            HttpResponse = Fault_Client_noNS_Begin +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                    Fault_noNS_End;
*/
            if (isDebugged)
                Controller_log.info("HttpResponse:" + HttpResponse);
            // Controller_log.warn("XML-HttpResponse готов" );

            try {
                JSONObject xmlJSONObj = XML.toJSONObject(  HttpResponse) ; // Parametrs_End + HttpResponse + Parametrs_End); // проверяли JSONException
                Controller_log.warn("JSON-HttpResponse построен" );
                String jsonPrettyPrintString;
                if (Queue_ID == 0L)
                    jsonPrettyPrintString = ClientIpHelper.jsonPrettyArray(xmlJSONObj, -1, Controller_log ); // Post возвращает только объект а не массив объектов
                else
                    jsonPrettyPrintString = xmlJSONObj.toString(2);
                //System.out.println("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                postResponse.setContentType("text/json;Charset=UTF-8");
                HttpResponse = jsonPrettyPrintString;
                        Controller_log.warn("JSON-HttpResponse готов [" + jsonPrettyPrintString + "]" );
                        if (isDebugged)
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, jsonPrettyPrintString, Controller_log);
                        try {
                            if (messageReceiveTask.theadDataAccess != null) {
                                if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                    messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                                messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                            }
                        } catch (SQLException SQLe) {
                            Controller_log.error(SQLe.getMessage());
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            System.err.println( strInterruptedException( SQLe ) );
                        }
                        Controller_log.info( "jsonPrettyPrint:[" + jsonPrettyPrintString +"] DataSourcePool=" + DataSourcePoolMetadata.getActive() );
                        postResponse.setContentType("text/json;Charset=UTF-8");

                        return ( HttpResponse );

            } catch (JSONException e) {
                //strInterruptedException( e );
                System.err.println( strInterruptedException( e ) );
                // Не смогли преобразовать HttpResponse в JSON
                HttpResponse = Fault_Server_Rest_Begin + "Не смогли преобразовать HttpResponse в JSON: " + e.getMessage() + Fault_Rest_End;
            }

            postResponse.setContentType("text/json;Charset=UTF-8");
            if (messageReceiveTask.theadDataAccess != null) {
                if (isDebugged)
                    messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, HttpResponse, Controller_log);
                try {
                    if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                        messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                    messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                } catch (SQLException SQLe) {
                    Controller_log.error(SQLe.getMessage());
                    Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                    System.err.println( strInterruptedException( SQLe ) );
                }
            }
            Controller_log.warn("HttpResponse: !!" + HttpResponse );
            return HttpResponse;
        }
        catch ( Exception RestAPI_ReceiveTaskE) {
            Controller_log.error( "RestAPI_ReceiveTask Exception: " + RestAPI_ReceiveTaskE.getMessage());
            System.err.println(strInterruptedException (RestAPI_ReceiveTaskE) ); //.printStackTrace();
            HttpResponse = Fault_Server_Rest_Begin + "RestAPI_ReceiveTask Exception: " + RestAPI_ReceiveTaskE.getMessage() + Fault_Rest_End;
        }
        finally {
            if ( messageReceiveTask != null)
                if (messageReceiveTask.theadDataAccess != null) {
                    try {
                        if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                        return HttpResponse;
                    }
                }
             return HttpResponse;
        }
    }

 //  {"/HermesService/PostHttpRequest/*", "/HermesService/SoapRequest/*"}
    @GetMapping(path = {"/MsgBusService/SoapRequest/*", "/HermesService/SoapRequest/*", "/HermesSOAPService/SOAPServlet/*"} , produces = MediaType.ALL_VALUE, consumes = MediaType.ALL_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    // для публикации WSDL|XSD
    public String GetHttpRequest(ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String queryString;
        String myHostAddress;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException | NullPointerException e) {
            if ( e instanceof java.lang.NullPointerException ) {
                queryString="";
            }
            else
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        try {
            myHostAddress = InetAddress.getLocalHost().getHostAddress();
        }
        catch ( UnknownHostException e) {
            Controller_log.error( "InetAddress.getLocalHost().getHostAddress() fault" + e.toString()) ;
            myHostAddress = e.getMessage();
        }
        Controller_log.warn("InetAddress.getLocalHost() : " + myHostAddress );

        String Url_Soap_Send = ClientIpHelper.findUrl_Soap_Send(url);
        String HttpResponse = Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_noNS_End;
        int Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        Controller_log.warn("Interface_id=" + Interface_id);
        if (Interface_id < 0) {
            getResponse.setStatus(500);
            HttpResponse = Fault_Client_noNS_Begin +
                    "Интерфейс для обработки " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                    " в системе не сконфигурирован" +
                    Fault_noNS_End;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            try {
                JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                String jsonPrettyPrintString = xmlJSONObj.toString(4);
                Controller_log.warn("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                getResponse.setContentType("text/json;Charset=UTF-8");
                return (jsonPrettyPrintString);

            } catch (JSONException e) {
                System.err.println(e.toString());
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            return HttpResponse;
        } else
        // Начинае подготовку к обработке запроса
        { StringBuilder tmpHttpResponse = new StringBuilder();

            tmpHttpResponse.append(Parametrs_Begin);
            String queryParams[];
            queryParams = queryString.split("&");
            for (int i= 0 ; queryString.length() > 1 && i < queryParams.length; i++)
            { // Controller_log.warn( queryParams[i]);
                String ParamElements[] = queryParams[i].split("=");
                Controller_log.warn( ParamElements[0] + " :=" );

                tmpHttpResponse.append(OpenTag);
                tmpHttpResponse.append(ParamElements[0]);
                tmpHttpResponse.append(CloseTag);

                if ( ParamElements[0] != null ) {
                    String UpperParamElement = ParamElements[0].toUpperCase();
                    String XSD_section=null;

                    if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                        Controller_log.warn(queryParams[i].substring(ParamElements[0].length() + 1));
                        tmpHttpResponse.append(queryParams[i].substring(ParamElements[0].length() + 1));
                        XSD_section = queryParams[i].substring(ParamElements[0].length() + 1) ;
                    }

                    // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
                    int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
                    if (MessageTemplateVOkey < 0)
                    { getResponse.setContentType("text/xml; charset=utf-8");
                        Controller_log.error("WSDL для интерфейса Id='" + Interface_id + "') не найден" );
                        String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                                "WSDL/XSD для интерфейса Id='" + Interface_id + "' не найден" +
                                Fault_End + Body_End + Envelope_End;
                        getResponse.setStatus(200);
                        return OutResponse;
                    }
                    Boolean isDebugged = false;
                    String getIsDebugedResponse = ClientIpHelper.getIsDebuged( MessageTemplateVOkey, isDebugged, Controller_log);
                    if ( ! getIsDebugedResponse.equalsIgnoreCase("Ok") )
                    {
                        getResponse.setContentType("text/xml; charset=utf-8");
                        getResponse.setStatus(500);
                        return getIsDebugedResponse;
                    }
                        switch (UpperParamElement) {
                            case WSDLhi:

                                    TextStringBuilder WsdlInterface = new TextStringBuilder( MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlInterface());
                                    WsdlInterface.replaceAll("http://10.32.245.8:7001/", "http://" + myHostAddress + ":8008/" );
                                    if ( isDebugged)
                                    Controller_log.info("WsdlInterface:" + WsdlInterface);
                                    if (WsdlInterface != null) {
                                        getResponse.setStatus(200);
                                        getResponse.setContentType("text/xml; charset=utf-8");
                                        return WsdlInterface.toString();
                                    }
                                    else {
                                        getResponse.setStatus(404);
                                        Controller_log.error("WSDL для интерфейса Id='" + Interface_id + "') не сконфигурён" );
                                        String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                                                "WSDL для интерфейса Id='" + Interface_id + "' не сконфигурён" +
                                                Fault_End + Body_End + Envelope_End;
                                        return OutResponse;
                                    }


                            case XSDhi:
                                String XSDInterface = null;
                                if ( XSD_section != null) {

                                    switch ( "WsdlXSD" + XSD_section  ) {
                                        case "WsdlXSD1":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD1();
                                            break;
                                        case "WsdlXSD2":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD2();
                                            break;
                                        case "WsdlXSD3":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD3();
                                            break;
                                        case "WsdlXSD4":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD4();
                                            break;
                                        case "WsdlXSD5":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD5();
                                            break;
                                        case "WsdlXSD6":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD6();
                                            break;
                                        case "WsdlXSD7":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD7();
                                            break;
                                        case "WsdlXSD8":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD8();
                                            break;
                                        case "WsdlXSD9":
                                            XSDInterface = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getWsdlXSD9();
                                            break;
                                    }
                                }
                                if ( XSDInterface != null) {
                                    getResponse.setStatus(200);
                                    getResponse.setContentType("text/xml; charset=utf-8");
                                    return XSDInterface;
                                }
                                else {
                                    getResponse.setContentType("text/xml; charset=utf-8");
                                    getResponse.setStatus(404);
                                    Controller_log.error("WsdlXSD [" + XSD_section +  "] для интерфейса Id='" + Interface_id + "') не найден" );
                                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                                            "WSDLXSD [" + XSD_section + "]  для интерфейса Id='" + Interface_id + "' не найден" +
                                            Fault_End + Body_End + Envelope_End;
                                    return OutResponse;
                                }
                        }

                }
                tmpHttpResponse.append(OpenTag);
                tmpHttpResponse.append(EndTag);
                tmpHttpResponse.append(ParamElements[0]);
                tmpHttpResponse.append(CloseTag);
            }
            tmpHttpResponse.append(Parametrs_End);
            Controller_log.info( tmpHttpResponse.toString());
            return  tmpHttpResponse.toString();
        }
    }

}
