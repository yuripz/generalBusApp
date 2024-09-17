package net.plumbing.msgbus.controller;

//import org.eclipse.jetty.server.Authentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
//import org.springframework.web.context.request.async.DeferredResult;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
//import javax.swing.text.html.parser.Entity;

import net.plumbing.msgbus.common.ClientIpHelper;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;


import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.sql.SQLException;

import static net.plumbing.msgbus.common.XMLchars.*;
import static net.plumbing.msgbus.common.ApplicationProperties.DataSourcePoolMetadata;

@RestController
//@RequestMapping("/HermesService") //extends RestAPI_ReceiveTask
public class GetController  {

    private static final Logger Controller_log = LoggerFactory.getLogger(GetController.class);

// "/HermesSOAPService/GetHttpRequest/*" - HE-10225, в синхронном ответе возвращать xml
    @GetMapping(path ={"/HermesService/GetHttpRequest/*", "/MsgBusService/GetHttpRequest/*","/HermesSOAPService/GetHttpRequest/*" }, produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    @CrossOrigin(origins = "*")
//    @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String GetHttpRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        // Controller_log.warn("GetHttpRequest->RemoteAddr: \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"" );
        String url = httpRequest.getRequestURL().toString();
        boolean is_TextJsonResponse=true;
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), StandardCharsets.UTF_8);
        } catch (NullPointerException | IllegalArgumentException e) {
            Controller_log.error( "httpRequest.getRequestURL `" + url + "` URLDecoder.decode `" + httpRequest.getQueryString() +" `fault "  + e.getMessage());
            System.err.println( "httpRequest.getRequestURL `" + url + "` URLDecoder.decode `" + httpRequest.getQueryString() +" `fault "  + e.getMessage());
            e.printStackTrace();
            queryString = httpRequest.getQueryString();
        }
        if ( url.indexOf("/HermesSOAPService/") > 0 )  is_TextJsonResponse=false;

        getResponse.addHeader("Access-Control-Allow-Origin", "*");
        //Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        String HttpResponse= Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_noNS_End;
        if ( queryString == null )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Клиент не передал " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " параметры в запросе" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            if (is_TextJsonResponse )
            {
                try {
                    JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                    String jsonPrettyPrintString = xmlJSONObj.toString(4);
                    Controller_log.warn("jsonPrettyPrintString:[" + jsonPrettyPrintString + "]");
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    return(jsonPrettyPrintString);

                } catch (JSONException e) {
                    System.err.println(e.toString());
                }
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            return HttpResponse;
        }
        String Url_Soap_Send = ClientIpHelper.findUrl_Soap_Send(url);

        int  Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        Controller_log.warn("look4MessageTypeVO_2_Interface ("+ Url_Soap_Send+ "): Interface_id=" + Interface_id );
        if ( Interface_id < 0 )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Интерфейс для обработки " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " в системе не сконфигурирован" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse: [" + HttpResponse + "]");
            if (is_TextJsonResponse ) {
                try {
                    JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                    String jsonPrettyPrintString = xmlJSONObj.toString(4);
                    Controller_log.warn("jsonPrettyPrintString:[" + jsonPrettyPrintString + "]");
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    return (jsonPrettyPrintString);

                } catch (JSONException e) {
                    System.err.println(e.toString());
                }
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            return HttpResponse;
        }
        else
            // Начинае подготовку к обработке запроса
        {
            MessageDetails Message = new MessageDetails();
            Message.XML_Request_Method.append(Parametrs_Begin);
            String[] queryParams;
            queryParams = queryString.split("&");
            for (int queryParamIndex = 0; queryParamIndex < queryParams.length; queryParamIndex++) { // Controller_log.warn( queryParams[i]);
                String[] ParamElements = queryParams[queryParamIndex].split("=");
                //Controller_log.warn(ParamElements[0]);
//-------------------------------------------------
                Message.XML_Request_Method.append(OpenTag);
                //Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                 Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);

                if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                    //Controller_log.warn(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                    // Если передан JSon, то пробуем превратить его в XML для обработки
                    ClientIpHelper.add2XML_Request_Method_CustomTags(Message.XML_Request_Method, queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1),  Controller_log);
                    // Message.XML_Request_Method.append(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                }

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(EndTag);
                // Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);
//-------------------------------------------------/
/*=============================================================
                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);

                if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                    // Controller_log.warn(queryParams[i].substring(ParamElements[0].length() + 1));
                    Message.XML_Request_Method.append(queryParams[queryParamIndex].substring(ParamElements[0].length() + 1));
                }

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(EndTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);
                //===================================================*/
            }
            Message.XML_Request_Method.append(Parametrs_End);
            Controller_log.info("input XML_Request_Method: [" + Message.XML_Request_Method.toString() + "]");

            Message.XML_MsgInput = Envelope_noNS_Begin
                    + Header_noNS_Begin + Header_noNS_End
                    + Body_noNS_Begin
                    + Message.XML_Request_Method.toString()
                    + Body_noNS_End + Envelope_noNS_End
            ;
            // Message.XML_MsgClear.append(Message.XML_MsgInput);
            MessageReceiveTask messageReceiveTask=null;
            try {
                messageReceiveTask = new MessageReceiveTask();
                // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
                int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
                boolean isDebugged = true ; // TODO: this.isDebugged=true; -- для Документирования // false
                Controller_log.warn("isDebugged before ClientIpHelper.getIsDebuged(): false" + isDebugged );
                String getIsDebugedResponse = ClientIpHelper.getIsDebuged( MessageTemplateVOkey, isDebugged, Controller_log);
                if ( ( ! getIsDebugedResponse.equalsIgnoreCase("true") ) && ( ! getIsDebugedResponse.equalsIgnoreCase("false") ))
                {
                    isDebugged = false;
                }
                else {
                    isDebugged = getIsDebugedResponse.equalsIgnoreCase("true");
                }
                Controller_log.warn("isDebugged after ClientIpHelper.getIsDebuged(): " + isDebugged );

                Long Queue_ID;
                Queue_ID = messageReceiveTask.ProcessInputMessage(Interface_id, Message, MessageTemplateVOkey, isDebugged);
                // Тут не закрываем соединение, оно нужно для журнала
                /*
                try {
                    if (messageReceiveTask.theadDataAccess != null) {
                        if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    }
                } catch (SQLException e) {
                    Controller_log.error("Проблемы с закрытием theadDataAccess соединения " + e.getMessage());
                    e.printStackTrace();
                }
                */
                if (Queue_ID == 0L) {
                   // String isRest;
                    getResponse.setStatus(200);
                    if ( MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(Url_Soap_Send, Controller_log))
                       // в URL_SOAP_Ack интерфейса записан REST, значит без <Body></Body>
                        HttpResponse = Message.XML_MsgResponse.toString();
                    else HttpResponse = Body_noNS_Begin +
                            Message.XML_MsgResponse.toString() +
                            Body_noNS_End;
                } else {
                    if (Queue_ID > 0L) {
                        getResponse.setStatus(500);
                        HttpResponse = Fault_Client_noNS_Begin +
                                XML.escape(Message.MsgReason.toString()) +
                                Fault_noNS_End;
                    } else {
                        getResponse.setStatus(500);
                        HttpResponse = Fault_Server_noNS_Begin +
                                Message.MsgReason.toString() +
                                Fault_noNS_End;
                    }
                }

                if (isDebugged)
                Controller_log.info("HttpResponse:[" + HttpResponse + "]");
                getResponse.setHeader("Access-Control-Allow-Origin", "*");
                getResponse.setContentType("application/json;Charset=UTF-8");
                // getResponse.setContentType("text/xml;charset=UTF-8");
                if (is_TextJsonResponse ) {
                    try {
                        JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);
                        // TODO внутри XML.toJSONObject метод stringToValue делает аналогично StringEscapeUtils.escapeJson()

                        String jsonPrettyPrintString = xmlJSONObj.toString(4); //StringEscapeUtils.unescapeXml (xmlJSONObj.toString(4) );
                        getResponse.setContentType("application/json;Charset=UTF-8");
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
                            SQLe.printStackTrace();
                        }
                        if (isDebugged)
                        Controller_log.warn( "jsonPrettyPrintString : " + jsonPrettyPrintString);
                        Controller_log.info("DataSourcePool " + DataSourcePoolMetadata.getActive());
                        return (jsonPrettyPrintString);

                    } catch (JSONException e) {
                        System.err.println(e.toString());
                    }
                }
              // возвращаем XML
                    if (messageReceiveTask.theadDataAccess != null) { // Закрываем соединение
                        if (isDebugged)
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, HttpResponse, Controller_log);
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
                getResponse.setContentType("text/xml;charset=UTF-8");
                return HttpResponse;

            } finally {
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

            }
        }


    }

    @GetMapping(path ={"/HermesService/PushMQRequest/*", "/MsgBusService/PushMQRequest/*"} ,produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String PushMessageRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        String url = httpRequest.getRequestURL().toString();
        String queryString;

        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");

        String HttpResponse= Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_noNS_End;

        String queryParams[];
        String sQueue_ID=null;
        Long Queue_ID = null;
        queryParams = queryString.split("&");
        for (int i= 0 ; i < queryParams.length; i++)
        { // Controller_log.warn( queryParams[i]);
            String ParamElements[] = queryParams[i].split("=");
            Controller_log.warn( ParamElements[0] );

            if ( ParamElements[0] != null) {
                if ( ParamElements[0].equalsIgnoreCase("QueueID")) {
                    if (( ParamElements.length>1) && (ParamElements[1] !=null )){
                        Controller_log.warn( queryParams[i].substring(ParamElements[0].length()+1 ));
                        sQueue_ID = queryParams[i].substring(ParamElements[0].length()+1 ) ;
                    }
                }
            }

        }
        if ( sQueue_ID != null ) {
            try {
                Queue_ID = Long.parseLong(sQueue_ID);
                HttpResponse= Fault_Client_noNS_Begin +
                        XML.escape(httpRequest.getMethod() + ": QueueID= (" + Queue_ID + ")") +
                        Fault_noNS_End;
            } catch ( NumberFormatException e) {
                HttpResponse= Fault_Client_noNS_Begin +
                        XML.escape(httpRequest.getMethod() + ": QueueID= (" + sQueue_ID + ") не может быть преобразовано в число!") +
                        Fault_noNS_End;
            }
            StringBuilder MsgReason = new StringBuilder();
            PushMessage2ActiveMQ push2ActiveMQ = new PushMessage2ActiveMQ();
            push2ActiveMQ.push_QeueId2ActiveMQqueue( Queue_ID.longValue(),  MsgReason );
            try {
                if ( push2ActiveMQ.theadDataAccess.Hermes_Connection != null )
                    push2ActiveMQ.theadDataAccess.Hermes_Connection.close();
                push2ActiveMQ.theadDataAccess.Hermes_Connection = null;
            } catch (SQLException SQLe) {
                Controller_log.error(SQLe.getMessage());
                Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                SQLe.printStackTrace();
            }
            HttpResponse= Fault_Client_noNS_Begin +
                    XML.escape(MsgReason.toString()) +
                    Fault_noNS_End;

        }
        else HttpResponse= Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ") , параметр QueueID не передан !") +
                Fault_noNS_End;

        return HttpResponse;
    }

    @GetMapping(path ={"/HermesService/InternalRestApi/**", "/MsgBusService/InternalRestApi/**"}, produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    @CrossOrigin(origins = "*")
   // @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String GetHermesRestApi(ServletRequest getServletRequest, HttpServletResponse getResponse, Authentication httpRequestAuthentication, Principal httpRequestUserPrincipal) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        Controller_log.warn("GetHermesRestApi-> RemoteAddr: `" + getServletRequest.getRemoteAddr() + "` ,RemoteHost: `" + getServletRequest.getRemoteHost() + "`");
        String url = httpRequest.getRequestURL().toString();
        String OperationId = httpRequest.getHeader("BusOperationId");
        String BusOperationMesssageType = null;
        String queryString;
        String ResponseStatus;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), StandardCharsets.UTF_8);
        } catch (NullPointerException e) {
            queryString = httpRequest.getQueryString();
        }
//        String username = SecurityContextHolder.getContext().getAuthentication().getName();
//        Controller_log.warn("SecurityContextHolder.getContext().getAuthentication().getName() = " + username );
//        // Principal httpRequestUserPrincipal = httpRequest.getUserPrincipal();
//        if (httpRequestUserPrincipal != null)
//        {
//            Controller_log.warn("httpRequestUserPrincipal = " + httpRequestUserPrincipal.getName() );
//        }
//        else Controller_log.warn("httpRequestUserPrincipal is null" );
//        if (httpRequestAuthentication != null)
//        {
//            Controller_log.warn("httpRequestauthentication = " + httpRequestAuthentication.toString() );
//        }
//        else Controller_log.warn("httpRequestauthentication is null" );

        // Controller_log.warn("BusOperationId= " + OperationId );
        getResponse.setHeader("Access-Control-Allow-Origin", "*");
        //Controller_log.warn("GetHermesRestApi : url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        String HttpResponse= Fault_Client_Rest_Begin +
                org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_Rest_End;

        String Url_Soap_Send = ClientIpHelper.findTypes_URL_SOAP_SEND(url, "InternalRestApi/", Controller_log);
        if ( Url_Soap_Send == null)
        {   getResponse.setStatus(422);
                HttpResponse= Fault_Client_Rest_Begin +
                        "Клиент не передал в " +
                        org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")  интерфейс для обработки" ) +
                        Fault_Rest_End ;
            getResponse.setContentType("application/json;Charset=UTF-8");
            return HttpResponse;
        }

        int  Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        Controller_log.warn("Interface_id=" + Interface_id );
        if ( Interface_id < 0 )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_Rest_Begin +
                    "Ресурса нет на сервере: Интерфейс для обработки " +
                    org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    // + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")"
                    " в системе не сконфигурирован" +
                    Fault_Rest_End ;
            Controller_log.warn("HttpResponse:" + HttpResponse);
            getResponse.setContentType("application/json;Charset=UTF-8");
            return HttpResponse;
        }

        // Начинае подготовку к обработке запроса
        if (OperationId == null )
        {
            BusOperationMesssageType = ClientIpHelper.find_BusOperationMesssageType(url, "InternalRestApi/" + Url_Soap_Send + "/", Controller_log);
            if (BusOperationMesssageType == null) {
                getResponse.setStatus(422);
                HttpResponse = Fault_Client_Rest_Begin +
                        "Клиент не передал в " +
                        org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") имя сущности (тип операции) для обработки") +
                        Fault_Rest_End;
                getResponse.setContentType("application/json;Charset=UTF-8");
                return HttpResponse;
            }
            Controller_log.warn("try look4MessageTypeVO_by_MesssageType: [" + BusOperationMesssageType + "]");
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
//                            getResponse.setContentType("application/json;Charset=UTF-8");
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
//                            getResponse.setContentType("application/json;Charset=UTF-8");
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
//                        getResponse.setContentType("application/json;Charset=UTF-8");
//                        getResponse.setStatus(422);
//                        return HttpResponse;
//                    }
//                    Controller_log.warn("Орерация с типом:" + BusOperationMesssageType + "GetList" + " NN=" + OperationId);
//                }

            OperationId = MessageRepositoryHelper.look4MessageTypeVO_by_MesssageType(BusOperationMesssageType, Interface_id, Controller_log);
        }


            if (OperationId == null) {
                if (MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface(Url_Soap_Send, Controller_log))
                {       // если на интерфейсе прописан REST-XML , то возвращаем XML
                    getResponse.setContentType("text/xml;charset=UTF-8");
                    HttpResponse= Fault_Client_noNS_Begin +
                            "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType  + " для обработки " +
                            org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
                            " в системе не сконфигурирована" +
                            Fault_noNS_End ;
                }
                else {  // возвращаем JSON
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    HttpResponse = Fault_Client_Rest_Begin +
                            "Ресурса нет на сервере: Орерация с типом " + BusOperationMesssageType  + " для обработки " +
                            org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ")") +
                            " в системе не сконфигурирована" +
                            Fault_Rest_End;
                }

                Controller_log.warn("HttpResponse:" + HttpResponse);
                getResponse.setStatus(422);
                return HttpResponse;
            }
            Controller_log.warn("Орерация с типом: `" + BusOperationMesssageType + "` NN=" + OperationId);

            if ( (queryString == null)  || (OperationId == null ))
            {   getResponse.setStatus(422);
                if (queryString == null)
                HttpResponse= Fault_Client_Rest_Begin +
                        "Клиент не передал " +
                        org.apache.commons.text.StringEscapeUtils.escapeJson(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                        " параметры в запросе" +
                        Fault_Rest_End ;
                if (OperationId == null)
                    HttpResponse= Fault_Client_Rest_Begin +
                            "Клиент не передал  в запросе HTTP-Header BusOperationId" +
                            Fault_Rest_End ;
                Controller_log.warn("HttpResponse:" + HttpResponse);
                getResponse.setContentType("application/json;Charset=UTF-8");
                return HttpResponse;
            }

            MessageDetails Message = new MessageDetails();

            Message.XML_Request_Method.append(Parametrs_Begin);
            String[] queryParams;
            queryParams = queryString.split("&");
            // filter={}&range=[0,9]&sort=["id","ASC"])
            for (int queryParamIndex = 0; queryParamIndex < queryParams.length; queryParamIndex++) { // Controller_log.warn( queryParams[i]);

                String[] ParamElements = queryParams[queryParamIndex].split("=");
                 Controller_log.warn(ParamElements[0]);

                // String ParamElementName = ClientIpHelper.toCamelCase(ParamElements[0], "_");
                //int ParamElementNameLength = (ParamElementName.indexOf(']') > 0) ? ParamElementName.indexOf(']') : ParamElementName.length();
                try { // ?_end=5&_order=DESC&_sort=username&_start=0
                    Controller_log.warn("ParamElements[0]=" + ParamElements[0] + " indexOf(Filter)=" + ParamElements[0].indexOf("Filter") );
                    if ( ParamElements[0].contains("Filter") )
                        try {
                            ClientIpHelper.add2XML_Request_Method_FilterTags(Message.XML_Request_Method, queryParamIndex, queryParams, ParamElements, Controller_log);
                        }
                      catch ( JSONException JSe) {
                          HttpResponse= Fault_Client_Rest_Begin +
                                  "Клиент передал  в запросе фильтр не в формате JSON, " + JSe.getMessage() +
                                  Fault_Rest_End ;
                          Controller_log.warn("HttpResponse: Filter=`" + ParamElements[0].indexOf("Filter")  + "` ==> " + HttpResponse);
                          getResponse.setContentType("application/json;Charset=UTF-8");
                          return HttpResponse;
                      }
                    else
                    {
                        Message.XML_Request_Method.append(OpenTag);
                        Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                        Message.XML_Request_Method.append(CloseTag);

                        if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                            //Controller_log.warn(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                            // Если передан JSon, то пробуем превратить его в XML для обработки
                            ClientIpHelper.add2XML_Request_Method_CustomTags(Message.XML_Request_Method, queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1),  Controller_log);
                            // Message.XML_Request_Method.append(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                        }

                        Message.XML_Request_Method.append(OpenTag);
                        Message.XML_Request_Method.append(EndTag);
                        Message.XML_Request_Method.append( ClientIpHelper.mapQryParam2SQLRequest(ParamElements[0]) );
                        Message.XML_Request_Method.append(CloseTag);
                    }

            }
                catch (StringIndexOutOfBoundsException | NumberFormatException e) {
                    // org.apache.commons.text.StringEscapeUtils.escapeJson();
                    // JSONObject json = new JSONObject("");
                    HttpResponse= Fault_Client_Rest_Begin + org.apache.commons.text.StringEscapeUtils.escapeJson(
                            "Ошибка при разборе параметров от клиента (" + queryString + ") " + e.getMessage() ) +
                            Fault_Rest_End ;
                    Controller_log.warn("HttpResponse:[" + HttpResponse + "]");
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    //getResponse.setHeader("x-total-count", "0" );
                    //getResponse.setStatus(422);
                    return HttpResponse;
                }
            }
            Message.XML_Request_Method.append(Parametrs_End);

        Message.XML_MsgInput = Envelope_noNS_Begin
                + Header_4BusOperationId_Begin + OperationId + Header_4BusOperationId_End
                + Body_noNS_Begin
                + Message.XML_Request_Method.toString()
                + Body_noNS_End + Envelope_noNS_End;

            Controller_log.info("Сформировали XML_MsgInput: `" + Message.XML_MsgInput.toString()+"`");
            // Message.XML_MsgClear.append(Message.XML_MsgInput);
            RestAPI_ReceiveTask messageReceiveTask=null;
            try {
                messageReceiveTask = new RestAPI_ReceiveTask();
                // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
                int MessageOperationId = Integer.parseInt(OperationId);
                // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
                int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
                boolean isDebugged = false;  // TODO: this.isDebugged=true; -- для Документирования false;

                Long Queue_ID = messageReceiveTask.ProcessRestAPIMessage(Interface_id, Message, MessageOperationId, isDebugged);

                if (Queue_ID == 0L) {
                    getResponse.setStatus(200); ResponseStatus="200";
                    if (MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(Url_Soap_Send, Controller_log))
                    {  if (isDebugged) Controller_log.info( "в URL_SOAP_Ack интерфейса записан REST, значит без <Body></Body>" );
                        if (MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_RestExel_2_Interface(Url_Soap_Send, Controller_log)) {
                            if (isDebugged)
                                Controller_log.info("на интерфейсе прописан `REST-EXCEL` , надо сказать браузеру, что возвращаем MML-файл в Excel-формат");
                            HttpResponse = """
                                    <?xml version="1.0" encoding="UTF-8"?>
                                    <?mso-application progid="Excel.Sheet"?>                                                                           
                                    """
                                    + Message.XML_MsgResponse.toString();
                        }
                        else HttpResponse = Message.XML_MsgResponse.toString();
                    }
                    // добавляем  <Body></Body>
                    else HttpResponse = Body_noNS_Begin +
                                        Message.XML_MsgResponse.toString() +
                                        Body_noNS_End;
                } else {
                    getResponse.setStatus(422);  ResponseStatus="422";
                    if (Queue_ID > 0L) {
                        getResponse.setStatus(500);  ResponseStatus="500";
                        HttpResponse = Fault_Client_noNS_Begin_4_Rest +
                                XML.escape(Message.MsgReason.toString()) +
                                Fault_noNS_End_4_Rest;
                    } else {
                        getResponse.setStatus(500); ResponseStatus="500";
                        HttpResponse = Fault_Server_noNS_Begin_4_Rest +
                                Message.MsgReason.toString() +
                                Fault_noNS_End_4_Rest;
                    }
                }

                if (isDebugged)
                    Controller_log.info("HttpResponse:`" + HttpResponse + "`");
                // Controller_log.warn("XML-HttpResponse готов" );

                if (MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface(Url_Soap_Send, Controller_log))
                {       // если на интерфейсе прописан REST-XML или REST-EXCEL, то возвращаем XML
                    if (isDebugged) Controller_log.info("на интерфейсе прописан REST-XML , то возвращаем XML" );
                    //getResponse.setContentType("text/xml;charset=UTF-8");
                    // HttpHeaders header;
                    //MediaType exelMediaType = new MediaType("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                    if (MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_RestExel_2_Interface(Url_Soap_Send, Controller_log)) {
                        // на интерфейсе прописан `REST-EXCEL` , надо сказать браузеру, что возвращаем MML-файл в Excel-формат
                        getResponse.setContentType("application/vnd.ms-excel");
                        // header.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=my_file.xls");
                        getResponse.setHeader("Content-Disposition", "attachment; filename=" + BusOperationMesssageType + ".xml");
                        //getResponse.setContentLength(HttpResponse.codePointCount(0, HttpResponse.length() ) );
                    }
                    else
                        getResponse.setContentType("text/xml;charset=UTF-8");

                }
                else
                {  // возвращаем JSON
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    try
                    {
                        String jsonPrettyPrintString;
                        if (HttpResponse.startsWith("<data/>")) {
                            jsonPrettyPrintString = "[]";
                            Controller_log.warn("пустой JSON-HttpResponse сформирован");
                        } else
                        {
                            JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse, true);
                            if (isDebugged) {
                             String jsonPrettyString =  xmlJSONObj.toString(4);
                             Controller_log.warn("[" + Queue_ID.toString() +  "] непустой JSON-HttpResponse построен:" + jsonPrettyString);
                            }

                            if (Queue_ID == 0L) {
                                Controller_log.warn("[" + Queue_ID.toString() +  "] try ClientIpHelper.jsonPrettyArray:[ " +  Message.X_Total_Count + " ]" );
                                jsonPrettyPrintString = ClientIpHelper.jsonPrettyArray(xmlJSONObj, Message.X_Total_Count,  Controller_log);
                            }
                            else
                                jsonPrettyPrintString = xmlJSONObj.toString(2);
                        }
                        //System.out.println("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                        // getResponse.setContentType("application/json;Charset=UTF-8");
                        if (isDebugged)
                            Controller_log.warn("JSON-HttpResponse готов:" + jsonPrettyPrintString);
                        if ((isDebugged) &&
                            (messageReceiveTask.theadDataAccess != null) &&
                            (messageReceiveTask.theadDataAccess.Hermes_Connection != null))
                            messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog(Message.ROWID_QUEUElog, Message.Queue_Id, jsonPrettyPrintString, Controller_log);
                        try {
                            // Закрываем соединение после логирования
                            if (messageReceiveTask.theadDataAccess != null) {
                                if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                                    messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                                messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                            }
                        } catch (SQLException SQLe) {
                            Controller_log.error("Hermes_Connection.close() fault:" + SQLe.getMessage());
                            SQLe.printStackTrace();
                        }
                        Controller_log.info("Response.Status=" +  //getResponse.getStatus() +
                                         "; DataSourcePool=" + DataSourcePoolMetadata.getActive());

                        getResponse.setHeader("Access-Control-Allow-Origin", "*");
//                    getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
//                    getResponse.setHeader("Access-Control-Expose-Headers", "Content-Range");
                        getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
//                    getResponse.setHeader("Custom-Bus-Range", "MessageDirections : 0-9/*");
//                    getResponse.setHeader("Content-Range","MessageDirections : 0-4/79");
//                    getResponse.setHeader("content-range","4");

                        getResponse.setHeader("x-total-count", String.valueOf(Math.abs(Message.X_Total_Count)));
                        if (isDebugged)
                            Controller_log.warn("HttpResponse `x-total-count`:" + Message.X_Total_Count);
                        getResponse.setHeader("Access-Control-Allow-Origin", "*");
                        // if (isDebugged)  Controller_log.warn(jsonPrettyPrintString);

                        //String jNoRecod = jsonPrettyPrintString.replace( "\"data\": {\"Record\":" , " \"data\" :");
                        //  jsonPrettyPrintString = jNoRecod.replace( "]}}", "], \"meta\": {\"total\": 4}  }" );
                        // no meta, total in X-Total-Count :
                        //jsonPrettyPrintString = jNoRecod.replace( "]}}", "]}" );


                        // no { data } no meta, total in X-Total-Count :
                        // String jNoRecod_Id = jsonPrettyPrintString.replace( "Record_Id", "id");
                        //////////////////////////////////
//                    String jNoRecod = jsonPrettyPrintString.replace( "{\"data\": {\"Record\": " , "");
//                    jsonPrettyPrintString = jNoRecod.replace( "]}}", "]" );
                        /////////////////////////////////

//                    Controller_log.warn(jsonPrettyPrintString);
                        if (isDebugged) Controller_log.info("return jsonPrettyPrintString:" + jsonPrettyPrintString);
                        return (jsonPrettyPrintString);

                    } catch (JSONException e) {
                        System.err.println(e.toString());
                        HttpResponse = Fault_Server_Rest_Begin +
                                org.apache.commons.text.StringEscapeUtils.escapeJson("Не смогли преобразовать HttpResponse=`" + HttpResponse + "` в JSON: " + e.getMessage()) + Fault_Rest_End;
                        getResponse.setStatus(500); ResponseStatus="500";
                    }

                }
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
                        SQLe.printStackTrace();
                    }
                }

                getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
                getResponse.setHeader("Access-Control-Expose-Headers", "Content-Range");

                if (isDebugged) Controller_log.info("return HttpResponse:" + HttpResponse);
                Controller_log.info("Response.Status=" + ResponseStatus + "DataSourcePool=" + DataSourcePoolMetadata.getActive());

                return HttpResponse;
            } finally {
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
                            // return HttpResponse;
                        }
                    }

            }



    }

    @GetMapping(path ="/HermesService/RaspberyRestApi/*", produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    @CrossOrigin(origins = "*")
    //@ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String GetHttpRowRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        Controller_log.warn("GetHttpRequest->RemoteAddr: \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"" );
        String url = httpRequest.getRequestURL().toString();
        boolean is_TextJsonResponse=true;
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException | NullPointerException e) {
            queryString = httpRequest.getQueryString();
        }
        if ( url.indexOf("/HermesSOAPService/") > 0 )  is_TextJsonResponse=false;

        getResponse.addHeader("Access-Control-Allow-Origin", "*");
        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        String HttpResponse= Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_noNS_End;
        if ( queryString == null )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Клиент не передал " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " параметры в запросе" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            if (is_TextJsonResponse )
            {
                try {
                    JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                    String jsonPrettyPrintString = xmlJSONObj.toString(4);
                    Controller_log.warn("jsonPrettyPrintString:|" + jsonPrettyPrintString + "|");
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    return(jsonPrettyPrintString);

                } catch (JSONException e) {
                    System.err.println(e.toString());
                }
            }
            getResponse.setContentType("text/html;charset=UTF-8");
            return HttpResponse;
        }
        String Url_Soap_Send = ClientIpHelper.findUrl_Soap_Send(url);

        int  Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        Controller_log.warn("Interface_id=" + Interface_id );
        if ( Interface_id < 0 )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Интерфейс для обработки " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " в системе не сконфигурирован" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            if (is_TextJsonResponse ) {
                try {
                    JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                    String jsonPrettyPrintString = xmlJSONObj.toString(4);
                    Controller_log.warn("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                    getResponse.setContentType("application/json;Charset=UTF-8");
                    return (jsonPrettyPrintString);

                } catch (JSONException e) {
                    System.err.println(e.toString());
                }
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            return HttpResponse;
        }
        else
        // Начинае подготовку к обработке запроса
        {
            MessageDetails Message = new MessageDetails();
            Message.XML_Request_Method.append(Parametrs_Begin);
            String queryParams[];
            queryParams = queryString.split("&");
            for (int i = 0; i < queryParams.length; i++) { // Controller_log.warn( queryParams[i]);
                String ParamElements[] = queryParams[i].split("=");
                Controller_log.warn(ParamElements[0]);

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);

                if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                    Controller_log.warn(queryParams[i].substring(ParamElements[0].length() + 1));
                    Message.XML_Request_Method.append(queryParams[i].substring(ParamElements[0].length() + 1));
                }

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(EndTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);
            }
            Message.XML_Request_Method.append(Parametrs_End);
            Controller_log.info(Message.XML_Request_Method.toString());

            Message.XML_MsgInput = Envelope_noNS_Begin
                    + Header_noNS_Begin + Header_noNS_End
                    + Body_noNS_Begin
                    + Message.XML_Request_Method.toString()
                    + Body_noNS_End + Envelope_noNS_End
            ;
            // Message.XML_MsgClear.append(Message.XML_MsgInput);
            MessageReceiveTask messageReceiveTask=null;
            try {
                messageReceiveTask = new MessageReceiveTask();
                // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
                int MessageTemplateVOkey = MessageRepositoryHelper.look4MessageTemplate_2_Interface(Interface_id, Controller_log);
                boolean isDebugged = false; // TODO: this.isDebugged=true; -- для Документирования

                Long Queue_ID;
                Queue_ID = messageReceiveTask.ProcessInputMessage(Interface_id, Message, MessageTemplateVOkey, isDebugged);

                try {
                    if (messageReceiveTask.theadDataAccess != null) {
                        if (messageReceiveTask.theadDataAccess.Hermes_Connection != null)
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    }
                } catch (SQLException e) {
                    Controller_log.error("Проблемы с закрытием theadDataAccess соединения " + e.getMessage());
                    e.printStackTrace();
                }
                if (Queue_ID == 0L) {
                    String isRest;
                    getResponse.setStatus(200);
                    if ( MessageRepositoryHelper.isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(Url_Soap_Send, Controller_log))
                        // в URL_SOAP_Ack интерфейса записан REST, значит без <Body></Body>
                        HttpResponse = Message.XML_MsgResponse.toString();
                    else HttpResponse = Body_noNS_Begin +
                            Message.XML_MsgResponse.toString() +
                            Body_noNS_End;
                } else {
                    if (Queue_ID > 0L) {
                        getResponse.setStatus(500);
                        HttpResponse = Fault_Client_noNS_Begin +
                                XML.escape(Message.MsgReason.toString()) +
                                Fault_noNS_End;
                    } else {
                        getResponse.setStatus(500);
                        HttpResponse = Fault_Server_noNS_Begin +
                                Message.MsgReason.toString() +
                                Fault_noNS_End;
                    }
                }
                getResponse.setStatus(200);
/*
            HttpResponse = Fault_Client_noNS_Begin +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                    Fault_noNS_End;
*/
                if (isDebugged)
                    Controller_log.info("HttpResponse:" + HttpResponse);
                getResponse.setHeader("Access-Control-Allow-Origin", "*");
                getResponse.setContentType("application/json;Charset=UTF-8");
                // getResponse.setContentType("text/xml;charset=UTF-8");
                if (is_TextJsonResponse ) {
                    try {
                        JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                        String jsonPrettyPrintString = xmlJSONObj.toString(4);
                        //System.out.println("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                        getResponse.setContentType("application/json;Charset=UTF-8");
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
                            SQLe.printStackTrace();
                        }
                        Controller_log.info("DataSourcePool " + DataSourcePoolMetadata.getActive());
                        return (jsonPrettyPrintString);

                    } catch (JSONException e) {
                        System.err.println(e.toString());
                    }
                }

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
                        SQLe.printStackTrace();
                    }
                }

                getResponse.setContentType("text/xml;charset=UTF-8");
                return HttpResponse;
            } finally {
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

            }
        }


    }

}
