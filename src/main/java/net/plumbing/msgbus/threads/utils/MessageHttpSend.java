package net.plumbing.msgbus.threads.utils;


import net.plumbing.msgbus.common.json.XML;
import org.slf4j.Logger;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;
//import javax.security.cert.CertificateException;
//import javax.security.cert.X509Certificate;

import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;

public class MessageHttpSend {

    public static SSLContext getSSLContext( StringBuilder MsgReason) {
        SSLContext sslContext;
        try {
             sslContext = new SSLContextBuilder()
                     .loadTrustMaterial(null, new TrustSelfSignedStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            e.printStackTrace();
            sslContext=null;
            MsgReason.setLength(0); MsgReason.trimToSize(); MsgReason.append(e.getMessage());
        }
        return sslContext;
    }

    public static int setHttpGetParams(String PropQueryPostExec, String string_Queue_ID, HashMap<String, String> papamsInXml, boolean IsDebugged, Logger MessegeSend_Log) {
        String[] queryParams;
        int nOfParams=0; papamsInXml.clear();
        queryParams = PropQueryPostExec.split("&");
        for (int i = 0; i < queryParams.length; i++) { // MessegeSend_Log.warn( queryParams[i]);
            String[] ParamElements = queryParams[i].split("=");

            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                if ( IsDebugged )
                MessegeSend_Log.warn(ParamElements[0] + "=" + queryParams[i].substring(ParamElements[0].length() + 1));
                papamsInXml.put(ParamElements[0], XML.escape( queryParams[i].substring(ParamElements[0].length() + 1) ) );
            }
            else {
                if ( IsDebugged )
                    MessegeSend_Log.warn(ParamElements[0]);
                if ( i == queryParams.length -1 ) // последний параметр
                    papamsInXml.put(ParamElements[0], string_Queue_ID );
                else
                     papamsInXml.put(ParamElements[0], "");
            }
            nOfParams += 1;
        }
        return nOfParams;
    }


}
