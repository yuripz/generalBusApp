package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate4Perform;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;
//import java.security.cert.CertificateException;
//import java.security.cert.X509Certificate;

//import javax.net.ssl.SSLContext;
//import javax.security.cert.CertificateException;
//import javax.security.cert.X509Certificate;

//import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
// import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;

public class MessageHttpSend {

    public static SSLContext getSSLContext(StringBuilder MsgReason)  {
        SSLContext sslContext;
        try {
        sslContext = SSLContext.getInstance("SSL"); // OR TLS
        sslContext.init(null, new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
        }, new SecureRandom());
        /*
        try {
             sslContext = new SSLContextBuilder()
                     .loadTrustMaterial(null, new TrustSelfSignedStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();*/
        } catch (KeyManagementException | NoSuchAlgorithmException  e) {
            e.printStackTrace();
            sslContext=null;
            MsgReason.setLength(0); MsgReason.trimToSize(); MsgReason.append(e.getMessage());
        }
        return sslContext;
    }

    public static int setHttpGetParams(long Queue_Id, String PropQueryPostExec, String string_Queue_ID, HashMap<String, String> papamsInXml, boolean IsDebugged, Logger MessegeSend_Log) {
        String[] queryParams;
        int nOfParams=0; papamsInXml.clear();
        queryParams = PropQueryPostExec.split("&");
        for (int i = 0; i < queryParams.length; i++) { // MessegeSend_Log.warn( queryParams[i]);
            String[] ParamElements = queryParams[i].split("=");

            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                if ( IsDebugged )
                MessegeSend_Log.warn( "[" + Queue_Id + "] " + ParamElements[0] + "=" + queryParams[i].substring(ParamElements[0].length() + 1));
                papamsInXml.put(ParamElements[0], XML.escape( queryParams[i].substring(ParamElements[0].length() + 1) ) );
            }
            else {
                if ( IsDebugged )
                    MessegeSend_Log.warn("[" + Queue_Id + "] " + ParamElements[0]);
                if ( i == queryParams.length -1 ) // последний параметр
                    papamsInXml.put(ParamElements[0], string_Queue_ID );
                else
                    papamsInXml.put(ParamElements[0], "");
            }
            nOfParams += 1;
        }
        return nOfParams;
    }

    public static String WebRestExecGET( HttpClient ApiRestHttpClient, String EndPointUrl, long Queue_Id, MessageTemplate4Perform messageTemplate4Perform,
                                           int ApiRestWaitTime, Logger MessageSend_Log )
            throws IOException, InterruptedException
    {
        String RestResponse = null;
        int restResponseStatus=0;
        String webEndPointUrl = EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id);

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .GET()
                    .uri( URI.create(webEndPointUrl))
                    .header("User-Agent", "msgBus/Java-21")
                    .header("Accept", "*/*")
                    .header("Connection", "close")
                    .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                    .build();
            HttpResponse<String> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
            RestResponse = RestResponseGet.body(); //.toString();

            restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

            if ( messageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("[" + Queue_Id + "] WebRestExec.GET(" + webEndPointUrl + ") httpStatus=[" + restResponseStatus + "] RestResponse=(`" + RestResponse + "`)");
        return RestResponse;
    }

    public static String WebRestExecPOSTJSON( HttpClient ApiRestHttpClient, String EndPointUrl, long Queue_Id, MessageTemplate4Perform messageTemplate4Perform,
                                              int ApiRestWaitTime, String json_4_Post, Logger MessageSend_Log )
            throws IOException, InterruptedException
    {
        String RestResponse = null;
        int restResponseStatus=0;
        String webEndPointUrl = EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id);

        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(json_4_Post))
                .uri( URI.create(webEndPointUrl))
                .header("User-Agent", "msgBus/Java-21")
                .header("Accept", "*/*")
                .header("Content-Type", "text/json;charset=UTF-8")
                .header("Connection", "close")
                .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                .build();
        HttpResponse<String> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
        RestResponse = RestResponseGet.body(); //.toString();

        restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

        if ( messageTemplate4Perform.getIsDebugged() )
            MessageSend_Log.info("[" + Queue_Id + "] WebJSONExec.POST(" + webEndPointUrl + ") httpStatus=[" + restResponseStatus + "] RestResponse=(`" + RestResponse + "`)");
        return RestResponse;
    }

    public static String WebRestExePostExec(HttpClient ApiRestHttpClient, String webEndPointUrl, long Queue_Id, MessageTemplate4Perform messageTemplate4Perform,
                                            int ApiRestWaitTime, Logger MessageSend_Log )
            throws IOException, InterruptedException
    {

        String RestResponse = null;
        int restResponseStatus=0;

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .GET()
                    .uri( URI.create( webEndPointUrl ))
                    .header("User-Agent", "msgBus/Java-21")
                    .header("Accept", "*/*")
                    .header("Connection", "close")
                    .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                    .build();
            HttpResponse<String> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
            RestResponse = RestResponseGet.body(); //.toString();

            restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

            if ( messageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("[" + Queue_Id + "] WebRestExePostExec.GET(" + webEndPointUrl + ") httpStatus=[" + restResponseStatus + "] RestResponse=(`" + RestResponse + "`)");
            return RestResponse;

    }


}
