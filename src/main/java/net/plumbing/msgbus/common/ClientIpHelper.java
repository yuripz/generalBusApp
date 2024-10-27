package net.plumbing.msgbus.common;

import jakarta.servlet.http.HttpServletRequest;
//import javax.validation.constraints.NotNull;

//import org.apache.commons.io.Charsets;
//import org.apache.commons.text.StringEscapeUtils;
import net.plumbing.msgbus.common.json.JSONArray;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplateVO;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Properties;


public class ClientIpHelper {
    public static String findUrl_Soap_Send( String url) {
        String [] Url_Soap_Send = url.split("/");
        if (Url_Soap_Send.length > 0 ) {
            if ((Url_Soap_Send.length > 1 ) && ( Url_Soap_Send[Url_Soap_Send.length - 1].equalsIgnoreCase("windows-1251")))
                return Url_Soap_Send[Url_Soap_Send.length - 2];
            else
                return Url_Soap_Send[Url_Soap_Send.length - 1];
            // return "Создание приложений с Spring Boot";
        }
        else
            return null;
    }
    public static String findTypes_URL_SOAP_SEND( @NotNull String url, final String partUrlInternalRestApi, Logger Controller_log) {
        int posInternalRestApi = url.indexOf(partUrlInternalRestApi);
//      Controller_log.warn("findTypes_URL_SOAP_SEND() posInternalRestApi:" + posInternalRestApi );
        if (posInternalRestApi > 0) // в переданном URL есть ссылка имеено на Внутреннее Rest-API
        {
            String[] Url_Soap_Send = url.substring(posInternalRestApi + partUrlInternalRestApi.length()).split("/");
          Controller_log.warn("url.substring:" + url.substring(posInternalRestApi + partUrlInternalRestApi.length()));
//            for (String sFrag : Url_Soap_Send) {
//                Controller_log.warn(sFrag);
//            }
            if ( Url_Soap_Send!=null )
                return Url_Soap_Send[0];
            else {
                Controller_log.warn(" В полученном URL в выделенном substring: {} не найден разделитель `/`" , url.substring(posInternalRestApi , partUrlInternalRestApi.length()));
                return null;
            }

            /*
            if (Url_Soap_Send.length > 0) {
                if (Url_Soap_Send.length > 1)
                    return Url_Soap_Send[Url_Soap_Send.length - 2];
                else
                    return Url_Soap_Send[Url_Soap_Send.length - 1];
                // return "Создание приложений с Spring Boot";
            } else
                return Url_Soap_Send[0];
            */
        } else
            return null;
    }

    public static String find_BusOperationMesssageType( String url, final String partUrlInternalRestApi, Logger Controller_log) {
        int posInternalRestApi = url.indexOf(partUrlInternalRestApi);
        //Controller_log.warn("find_BusOperationId() posInternalRestApi:" + posInternalRestApi );
        if (posInternalRestApi > 0) // в переданном URL есть ссылка имеено на Внутреннее Rest-API
        {
            String[] Url_Soap_Send = url.substring(posInternalRestApi + partUrlInternalRestApi.length()).split("/");
           //Controller_log.warn("url.substring:" + url.substring(posInternalRestApi + partUrlInternalRestApi.length()));
//           for (String sFrag : Url_Soap_Send) {
//                Controller_log.warn(sFrag);
//            }
            if ( Url_Soap_Send!=null )
                return Url_Soap_Send[0];
            else
                return null;
            /*
            if (Url_Soap_Send.length > 0) {
                if (Url_Soap_Send.length > 1)
                    return Url_Soap_Send[Url_Soap_Send.length - 2];
                else
                    return Url_Soap_Send[Url_Soap_Send.length - 1];
                // return "Создание приложений с Spring Boot";
            } else
                return Url_Soap_Send[0];
            */
        } else
            return null;
    }

    public static String getClientIp(HttpServletRequest request) {
        String clientIp = request.getHeader("X-FORWARDED-FOR");
        if (clientIp == null) {
            clientIp = request.getRemoteAddr();
        } else {
            clientIp = clientIp.split(",")[0].trim();
        }
        return clientIp;
    }

    public static String getRealIP(HttpServletRequest request) {
        String clientIp = request.getHeader("X-Real-IP");
        if (clientIp == null) {
            clientIp = request.getRemoteAddr();
        }
        return clientIp;
    }
    public static String getIsDebuged(int MessageTemplateVOkey, boolean isDebugged, Logger Controller_log ) {
        String ConfigExecute = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getConfigExecute();
        if (isDebugged) Controller_log.info("ConfigExecute:" + ConfigExecute);
        if (ConfigExecute != null) {
            String isDebuggedStr= "false";
            Properties properties = new Properties();
            InputStream propertiesStream = new ByteArrayInputStream(ConfigExecute.getBytes(StandardCharsets.UTF_8));
            try {
                properties.load(propertiesStream);
                for (String key : properties.stringPropertyNames()) {
                    if (key.equals(MessageTemplateVO.PropDebug)) {
                        Controller_log.info("PropDebug Property[" + key + "]=[" + properties.getProperty(key) + "]");
                        if ((properties.getProperty(key).equalsIgnoreCase("on")) ||
                                (properties.getProperty(key).equalsIgnoreCase("full"))
                        ) {
                            isDebuggedStr = "true";
                        }
                        if ((properties.getProperty(key).equalsIgnoreCase("ON")) ||
                                (properties.getProperty(key).equalsIgnoreCase("FULL"))
                        ) {
                            isDebuggedStr = "true";
                        }
                    }
                }
            } catch (IOException ioException) {
                // postResponse.setStatus(500);
                ioException.printStackTrace(System.err);
                Controller_log.error("properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage());
                String OutResponse = XMLchars.Envelope_Begin + XMLchars.Empty_Header + XMLchars.Body_Begin + XMLchars.Fault_Client_Begin +
                        "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                        XMLchars.Fault_End + XMLchars.Body_End + XMLchars.Envelope_End;
                return OutResponse;
            }
            return isDebuggedStr;
        }
        else return "false";

    }

    public static String toCamelCase(final String init, final String separator) {
        if ( (init == null) || (separator == null))
            return null;

        final StringBuilder ret = new StringBuilder(init.length());

        for (final String word : init.split(separator)) {
            if (!word.isEmpty()) {
                ret.append(Character.toUpperCase(word.charAt(0)));
                ret.append(word.substring(1).toLowerCase());
            }
            if (!(ret.length() == init.length()))
                ret.append(separator);
        }

        return ret.toString();
    }

    public static void add2XML_Request_Method_RangeTags(StringBuilder XML_Request_Method, int queryParamIndex,  String queryParams[], String ParamElements[], Logger Controller_log )
            throws  StringIndexOutOfBoundsException,  NumberFormatException
    {
        String ParamElementName = ClientIpHelper.toCamelCase( ParamElements[0] , "_" ) ;
       // int ParamElementNameLength = ( ParamElementName.indexOf(']') > 0) ? ParamElementName.indexOf(']') : ParamElementName.length() ;

        if ( ParamElementName.equalsIgnoreCase("Range"))
        {
            Integer iFirstRecord2Fetch=0;
            Integer iLastRecord2Fetch=9;
            //Controller_log.warn(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
            String RangeRecord2Fetch = queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1);
            XML_Request_Method.append(XMLchars.OpenTag);
            XML_Request_Method.append( "FirstRecord2Fetch"  ) ;
            XML_Request_Method.append(XMLchars.CloseTag);
            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {

                // извлекаем из [0,9] первое число после "[" до ","
                iFirstRecord2Fetch = Integer.parseInt( RangeRecord2Fetch.substring(
                        RangeRecord2Fetch.indexOf('[') +1,
                        RangeRecord2Fetch.indexOf(',')
                ).trim() );
                iFirstRecord2Fetch ++;
                XML_Request_Method.append( iFirstRecord2Fetch.toString() );
//                XML_Request_Method.append( FirstRecord2Fetch.substring(
//                        FirstRecord2Fetch.indexOf('[') +1,
//                        FirstRecord2Fetch.indexOf(',')
//                ) ) ;
            }

            XML_Request_Method.append(XMLchars.OpenTag);
            XML_Request_Method.append(XMLchars.EndTag);
            XML_Request_Method.append("FirstRecord2Fetch" ) ;
            XML_Request_Method.append(XMLchars.CloseTag);

            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                XML_Request_Method.append(XMLchars.OpenTag);
                XML_Request_Method.append( "Page_Size"  ) ;
                XML_Request_Method.append(XMLchars.CloseTag);

                // извлекаем из [0,9] первое число после "," до "]"
                iLastRecord2Fetch = Integer.parseInt( RangeRecord2Fetch.substring(
                        RangeRecord2Fetch.indexOf(',') +1,
                        RangeRecord2Fetch.indexOf(']')
                ).trim() );
                Integer PageSize = iLastRecord2Fetch + 1 - iFirstRecord2Fetch + 1  ; // с учетом iFirstRecord2Fetch ++;
                XML_Request_Method.append( PageSize.toString());
                XML_Request_Method.append(XMLchars.OpenTag);
                XML_Request_Method.append(XMLchars.EndTag);
                XML_Request_Method.append( "Page_Size"  ) ;
                XML_Request_Method.append(XMLchars.CloseTag);

            }

        }
    }
// ?_end=5&_order=DESC&_sort=username&_start=0
    public static String mapQryParam2SQLRequest ( String QryPatam ) {
        switch ( QryPatam) {
            case  "_sort" : return ("Sort");
            case  "_order" : return ("OrderBy");
            case  "_start" : return ("FirstRecord2Fetch");
            case  "_end" : return ("LastRecord2Fetch");
            case  "_PkField" : return ("Pk_FieldName");
            case  "_PkValue" : return ("Pk_Value");
            case "_usrToken" : return ("Usr_Token");
            case  "Id" : return ("Id");
        }
        return "x_" + QryPatam;
       // return "_UNDEFINED_";
    }

    // {"data": {"Record": { }}} ==> [{}]
    // {"data": {"Record": [ { } ] }} => [{}]
    public static String jsonPrettyArray (JSONObject XML_MsgResponse , Integer X_Total_Count , Logger Controller_log )
    throws JSONException {
        JSONObject data = null;
       try {
           try {
               data = XML_MsgResponse.getJSONObject("data");
           }
           catch (JSONException e) {
               return org.apache.commons.text.StringEscapeUtils.escapeJson("{ \"error\" :" + e.getMessage() + " }" );
           }
           // if ( Record.optJSONArray() )
           JSONArray recordJSONArray = data.optJSONArray("Record");
           if ( X_Total_Count >= 0 ) {

               StringBuilder jsonArray = new StringBuilder("[");
               if (recordJSONArray != null) {
                   JSONObject currJSONObject;
                   for (int i = 0; i < recordJSONArray.length(); i++) {
                       currJSONObject = (JSONObject) recordJSONArray.get(i);
                       // Controller_log.warn("Record [ " + i + " ]" + currJSONObject.toString(2));
                       jsonArray.append(currJSONObject.toString(2));
                       if (i < (recordJSONArray.length() - 1))
                           jsonArray.append(',');
                   }
               } else {
                   JSONObject Record = data.getJSONObject("Record");
                   // Controller_log.warn("Record [ recordJSONArray=null ]" + Record.toString(2));
                   jsonArray.append(Record.toString(2));
               }
               jsonArray.append(']');
               return jsonArray.toString();

           }
           else
           {
               StringBuilder jsonArray = new StringBuilder();
               if (recordJSONArray != null) {
                   JSONObject currJSONObject = (JSONObject) recordJSONArray.get(0);
                   //Controller_log.warn("Record [ X_Total_Count < 0 ]" + currJSONObject.toString(2));
                   jsonArray.append(currJSONObject.toString(2));
               } else {
                   JSONObject Record = data.getJSONObject("Record");
                   //Controller_log.warn("Record [ X_Total_Count < 0, recordJSONArray=null ]" + Record.toString(2));
                   jsonArray.append(Record.toString(2));
               }

               return jsonArray.toString();
           }
       }
       catch (JSONException e) {
           e.printStackTrace();
           throw e;
       }
       // return null;
    }


    public static void add2XML_Request_Method_FilterTags(StringBuilder XML_Request_Method, int queryParamIndex,  String queryParams[], String ParamElements[], Logger Controller_log )
            throws  StringIndexOutOfBoundsException, JSONException
    {
            XML_Request_Method.append(XMLchars.OpenTag);
            XML_Request_Method.append( "RecordFilters"  ) ;
            XML_Request_Method.append(XMLchars.CloseTag);
            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {

                Controller_log.warn("FilterRecord2Fetch=" +  queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
                 // FilterRecord2Fetch={"Msgdirection_Cod":"SIBIR"}
                String FilterRecord2Fetch = queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1);
                if ( ( FilterRecord2Fetch != null) && (FilterRecord2Fetch.length() > 4) )
                {

                    JSONObject FilterRecordJSONObj = new  JSONObject( FilterRecord2Fetch );
                    Iterator<String> FilterRecordJSONs = FilterRecordJSONObj.keys();
                    while(FilterRecordJSONs.hasNext()) {
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append( "RecordFilter"  ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);
                        String JSONkey = (String) FilterRecordJSONs.next();
                        Object currJSONObject = FilterRecordJSONObj.get(JSONkey);

                        String JSONvalue = customReplaceAll(currJSONObject.toString(),"\n", ""); // Убираем из филра переводы строки
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append( "RecordFilterFieldName"  ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);
                        XML_Request_Method.append(JSONkey);
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append(XMLchars.EndTag);
                        XML_Request_Method.append( "RecordFilterFieldName"  ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append( "RecordFilterFieldValue"  ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);


                        //  вместо конкотенации парсим вложенный JSON
                        //   XML_Request_Method.append(JSONvalue); // .replace('\"', '\''));
                        // вместо конкотенации парсим вложенный JSON
                        if (JSONvalue.length() > 3 ) {
                            if (( JSONvalue.charAt(0) == '{') || ( JSONvalue.charAt(0) == '[') ) {

                                String JSONvalue_4_Parsing;
                                if ( JSONvalue.charAt(0) == '{') JSONvalue_4_Parsing = JSONvalue;
                                else JSONvalue_4_Parsing = "{ \"" + JSONkey + "s\":  " + JSONvalue + " }";
                                try {
                                    JSONObject RestResponseJSON = new JSONObject(JSONvalue_4_Parsing);
                                    XML.setMessege_Log( Controller_log );
                                    XML_Request_Method.append( XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse ) );

                                } catch (Exception e) {
                                    XML_Request_Method.append(JSONvalue);
                                    Controller_log.error("add2XML_Request_Method_FilterTags on `" + JSONvalue_4_Parsing + "` fault: " + sStackTrace.strInterruptedException(e));
                                    XML_Request_Method.append(  StringEscapeUtils.escapeXml10(MessageUtils.stripNonValidXMLCharacters(JSONvalue)) );
                                }
                            }
                            else // - это не Json, добавляем строку как есть
                                XML_Request_Method.append(  StringEscapeUtils.escapeXml10(MessageUtils.stripNonValidXMLCharacters(JSONvalue)) );
                            // int ParamElementNameLength = ( ParamElementName.indexOf(']') > 0) ? ParamElementName.indexOf(']') : ParamElementName.length() ;

                        }
                        else // - это не может быть Json добавляем строку как есть
                            XML_Request_Method.append( MessageUtils.stripNonValidXMLCharacters( JSONvalue) );
                        ////////
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append(XMLchars.EndTag);
                        XML_Request_Method.append( "RecordFilterFieldValue"  ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);
                        XML_Request_Method.append(XMLchars.OpenTag);
                        XML_Request_Method.append(XMLchars.EndTag);
                        XML_Request_Method.append("RecordFilter" ) ;
                        XML_Request_Method.append(XMLchars.CloseTag);
                    }
                }

            }

            XML_Request_Method.append(XMLchars.OpenTag);
            XML_Request_Method.append(XMLchars.EndTag);
            XML_Request_Method.append("RecordFilters" ) ;
            XML_Request_Method.append(XMLchars.CloseTag);

    }

    private static String customReplaceAll(String str, String oldStr, String newStr) {

        if ("".equals(str) || "".equals(oldStr) || oldStr.equals(newStr)) {
            return str;
        }
        if (newStr == null) {
            newStr = "";
        }
        final int strLength = str.length();
        final int oldStrLength = oldStr.length();
        StringBuilder builder = new StringBuilder(str);

        for (int i = 0; i < strLength; i++) {
            int index = builder.indexOf(oldStr, i);

            if (index == -1) {
                if (i == 0) {
                    return str;
                }
                return builder.toString();
            }
            builder = builder.replace(index, index + oldStrLength, newStr);

        }
        return builder.toString();
    }
    public static void add2XML_Request_Method_CustomTags(StringBuilder XML_Request_Method, String QryParam, String ParamElement,  Logger Controller_log )
            throws  StringIndexOutOfBoundsException
    {
        Controller_log.warn("add2XML_Request_Method_CustomTags:`" + ParamElement + "` escapeXml10(stripNonValidXMLCharacters()) : `" +  StringEscapeUtils.escapeXml10(MessageUtils.stripNonValidXMLCharacters(ParamElement)) + "`"  );
        String ClearParamElement;
        Long ParamElement2Long;
        switch ( QryParam) {
                case  "Sort" :
                case  "OrderBy" :
                case  "Pk_FieldName" :ClearParamElement = customReplaceAll(customReplaceAll(customReplaceAll(ParamElement, "union " , ""), "select ", ""), "\n", "");
            break;
                case  "FirstRecord2Fetch" :
                case  "LastRecord2Fetch" : ParamElement2Long = Long.parseLong(ParamElement); ClearParamElement = ParamElement2Long.toString();
            break;
                case "Usr_Token" : ClearParamElement =ParamElement.replaceAll("[^\\h]+", "");
            break;
                case  "Pk_Value" :
                case  "Id" :
                default: ClearParamElement =ParamElement;
            break;
        }

    if (ClearParamElement.length() > 3 ) {
        if (( ClearParamElement.charAt(0) == '{') || ( ClearParamElement.charAt(0) == '[') ) {
            try {
                JSONObject RestResponseJSON = new JSONObject(ClearParamElement);
                XML.setMessege_Log( Controller_log );

                XML_Request_Method.append( XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse ) );

            } catch (Exception e) {
                XML_Request_Method.append(ClearParamElement);
                Controller_log.error("add2XML_Request_Method_CustomTags on `" + XML_Request_Method + "` fault: " + sStackTrace.strInterruptedException(e));
                return;
            }
        }
        else // - это не Json, добавляем строку как есть
        XML_Request_Method.append(  StringEscapeUtils.escapeXml10(MessageUtils.stripNonValidXMLCharacters(ClearParamElement)) );
        // int ClearParamElementNameLength = ( ClearParamElementName.indexOf(']') > 0) ? ClearParamElementName.indexOf(']') : ClearParamElementName.length() ;

    }
    else // - это не может быть Json добавляем строку как есть
        XML_Request_Method.append( MessageUtils.stripNonValidXMLCharacters( ClearParamElement) );
    }
// Sort
//public static void add2XML_Request_Method_SortTags(StringBuilder XML_Request_Method, int queryParamIndex,  String queryParams[], String ParamElements[], Logger Controller_log )
//throws  StringIndexOutOfBoundsException
//{
//    String ParamElementName = ClientIpHelper.toCamelCase( ParamElements[0] , "_" ) ;
//    // int ParamElementNameLength = ( ParamElementName.indexOf(']') > 0) ? ParamElementName.indexOf(']') : ParamElementName.length() ;
//
//    if ( ParamElementName.equalsIgnoreCase("Sort"))
//    {
//        XML_Request_Method.append(XMLchars.OpenTag);
//        XML_Request_Method.append( "Sort"  ) ;
//        XML_Request_Method.append(XMLchars.CloseTag);
//        if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
//            //Controller_log.warn(queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1));
//            String SortField_and_Order = queryParams[ queryParamIndex ].substring(ParamElements[0].length() + 1);
//            // извлекаем из [0,9] первое число после "{" до "}"
//            XML_Request_Method.append( SortField_and_Order.substring(
//                    SortField_and_Order.indexOf("[\"") +2,
//                    SortField_and_Order.indexOf("\",\"")
//            ) ) ;
//        }
//
//        XML_Request_Method.append(XMLchars.OpenTag);
//        XML_Request_Method.append(XMLchars.EndTag);
//        XML_Request_Method.append("Sort" ) ;
//        XML_Request_Method.append(XMLchars.CloseTag);
//
//        if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
//            XML_Request_Method.append(XMLchars.OpenTag);
//            XML_Request_Method.append( "Order_By"  ) ;
//            XML_Request_Method.append(XMLchars.CloseTag);
//
//            //Controller_log.warn(queryParams[ queryParamIndex].substring(ParamElements[0].length() + 1));
//            String SortField_and_Order = queryParams[queryParamIndex ].substring(ParamElements[0].length() + 1);
//            // извлекаем из [0,9] первое число после "[" до ","
//            XML_Request_Method.append( SortField_and_Order.substring(
//                    SortField_and_Order.indexOf("\",\"") +3,
//                    SortField_and_Order.indexOf("\"]")
//                    )
//            );
//            XML_Request_Method.append(XMLchars.OpenTag);
//            XML_Request_Method.append(XMLchars.EndTag);
//            XML_Request_Method.append( "Order_By"  ) ;
//            XML_Request_Method.append(XMLchars.CloseTag);
//
//        }
//    }
//}

//    public static String BrecketString2CamelCase(final String init, final String separator) {
//        if ( (init == null) || (separator == null))
//            return null;
//        int init_length = init.indexOf(']');
//        if ( init_length < 1 ) init_length = init.length()  ;
//       //  else init_length = init_length -1;
//
//
//        final StringBuilder ret = new StringBuilder( init_length );
//        // берем из строки до ] ! , уменьшаем строку на 1
//
//        for (final String word : init.substring(0, init_length ).split(separator)) {
//            if (!word.isEmpty()) {
//                ret.append(Character.toUpperCase(word.charAt(0)));
//                ret.append(word.substring(1).toLowerCase());
//            }
//            if (!(ret.length() == init_length))
//                ret.append(separator);
//        }
//
//        return ret.toString();
//    }

}
