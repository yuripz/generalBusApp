package net.plumbing.msgbus.controller;

import com.google.common.io.CharStreams;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.model.MessageDetails;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
//import org.springframework.web.context.request.async.DeferredResult;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import net.plumbing.msgbus.common.ClientIpHelper;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;


import java.io.*;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

import static net.plumbing.msgbus.common.XMLchars.*;
import static net.plumbing.msgbus.common.ApplicationProperties.DataSourcePoolMetadata;
import static net.plumbing.msgbus.common.sStackTrace.strInterruptedException;
import static net.plumbing.msgbus.model.MessageTemplateVO.PropDebug;

@RestController
public class PutController {
    public static final Logger Controller_log = LoggerFactory.getLogger(PutController.class);
    @PutMapping(path = {"/HermesService/InternalRestApi/apiSQLRequest/**"}, produces = MediaType.ALL_VALUE, consumes = MediaType.ALL_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public
    String PutHermesRestApi(ServletRequest postServletRequest, HttpServletResponse postResponse)
    {
        InputStream inputStream = null;
        HttpServletRequest httpRequest = (HttpServletRequest) postServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String Url_Soap_Send = ClientIpHelper.findUrl_Soap_Send(url);
        String OperationId = httpRequest.getHeader("BusOperationId");

        Controller_log.info("PutHttpRequest: from \"" + ClientIpHelper.getClientIp(httpRequest) + "\" url= (" + url + ") BusOperationId:" + OperationId + " Real-IP:" + ClientIpHelper.getRealIP(httpRequest));
        String HttpResponse= Fault_Client_Rest_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ")") +
                Fault_Rest_End;

        postResponse.setHeader("Access-Control-Allow-Origin", "*");
        if ( OperationId == null )
        {   postResponse.setStatus(500);
            HttpResponse= Fault_Client_Rest_Begin +
                    "Клиент не передал  в запросе HTTP-Header BusOperationId" +
                    Fault_Rest_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);

            postResponse.setContentType("application/json;Charset=UTF-8");
            return HttpResponse;
        }

        Integer Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);

        try {
            inputStream = postServletRequest.getInputStream();
        } catch (IOException ioException) {
            postResponse.setStatus(500);
            ioException.printStackTrace(System.err);
            String OutResponse = Fault_Client_Rest_Begin +
                    "postServletRequest.getInputStream fault:" + ioException.getMessage() +
                    Fault_Rest_End;
            postResponse.setContentType("application/json;Charset=UTF-8");
            return OutResponse;
        }

        int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);

        boolean isDebugged = false;

        if (MessageTemplateVOkey >= 0)
        {
            String ConfigExecute = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getConfigExecute();
            Controller_log.info("ConfigExecute:" + ConfigExecute);
            if (ConfigExecute != null) {
                Properties properties = new Properties();
                InputStream propertiesStream = new ByteArrayInputStream(ConfigExecute.getBytes(StandardCharsets.UTF_8));
                try {
                    properties.load(propertiesStream);
                    for (String key : properties.stringPropertyNames()) {
                        if (key.equals(PropDebug)) {
                            Controller_log.info("PropDebug Property[" + key + "]=[" + properties.getProperty(key) + "]");
                            if ((properties.getProperty(key).equalsIgnoreCase("on")) ||
                                    (properties.getProperty(key).equalsIgnoreCase("full"))
                            ) {
                                isDebugged = true;
                            }
//                            if ((properties.getProperty(key).equalsIgnoreCase("ON")) ||
//                                    (properties.getProperty(key).equalsIgnoreCase("FULL"))
//                            ) {
//                                isDebugged = true;
//                            }
                        }
                    }
                } catch (IOException ioException) {
                    postResponse.setStatus(500);
                    ioException.printStackTrace(System.err);
                    Controller_log.error("properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage());
                    String OutResponse = Fault_Client_Rest_Begin +
                            "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                            Fault_Rest_End;
                    return OutResponse;
                }
            }
        }

        MessageDetails Message = new MessageDetails();
        //PropEncoding_In = "UTF-8";
        try (InputStreamReader reader = new InputStreamReader(inputStream, Charsets.toCharset ("UTF-8"));// Charsets.UTF_8)
        ) {

            Message.XML_MsgConfirmation.append( CharStreams.toString(reader) );
            JSONObject postJSONObject = new JSONObject( Message.XML_MsgConfirmation.toString() );
            Message.XML_MsgConfirmation.setLength(0);
            Message.XML_MsgConfirmation.trimToSize();
            Message.XML_MsgConfirmation.append( XML.toString( postJSONObject  , "Parametrs" ) );
            Message.XML_MsgInput = Envelope_noNS_Begin
                    + Header_noNS_Begin + Header_noNS_End
                    + Body_noNS_Begin
                    + Message.XML_MsgConfirmation.toString()
                    + Body_noNS_End + Envelope_noNS_End
            ;

            if ( isDebugged )
                Controller_log.warn("InputStreamReader to Message.XML_MsgInput[" +  Message.XML_MsgInput + "]");
            inputStream.close();
            // throw  new IOException( " Проверка ioException ! " ) ;
        } catch (IOException | JSONException| IllegalCharsetNameException ioException ) {
            postResponse.setStatus(500);
            Controller_log.error("CharStreams.toString(" +  Message.XML_MsgConfirmation.toString() + ") fault:" + ioException.getMessage());

            String OutResponse = Fault_Server_Rest_Begin + "CharStreams.toString(getInputStream) fault:" + ioException.getMessage()
                    + Fault_Rest_End;
            postResponse.setContentType("application/json;Charset=UTF-8");
            return OutResponse;

        }
        // очищаем использованный XML_MsgConfirmation
        Message.XML_MsgConfirmation.setLength(0);
        Message.XML_MsgConfirmation.trimToSize();

        RestAPI_ReceiveTask messageReceiveTask=null;
        try {
            messageReceiveTask = new RestAPI_ReceiveTask();
            // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
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
                String jsonPrettyPrintString = xmlJSONObj.toString(2);
                //System.out.println("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                postResponse.setContentType("application/json;Charset=UTF-8");
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
                postResponse.setContentType("application/json;Charset=UTF-8");

                return ( HttpResponse );

            } catch (JSONException e) {
                //strInterruptedException( e );
                System.err.println( strInterruptedException( e ) );
                // Не смогли преобразовать HttpResponse в JSON
                HttpResponse = Fault_Server_Rest_Begin + "Не смогли преобразовать HttpResponse в JSON: " + e.getMessage() + Fault_Rest_End;
            }

            postResponse.setContentType("application/json;Charset=UTF-8");
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

}
