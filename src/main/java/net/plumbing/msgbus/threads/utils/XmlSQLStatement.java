package net.plumbing.msgbus.threads.utils;

//import oracle.jdbc.OracleCallableStatement;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageDetails4Send;
import net.plumbing.msgbus.threads.TheadDataAccess;
import oracle.jdbc.OracleResultSet;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.postgresql.jdbc.PgResultSet;
//import oracle.jdbc.OracleResultSetMetaData;
//import oracle.jdbc.OracleTypes;
import org.apache.commons.text.StringEscapeUtils;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
//import org.jdom2.filter.Filters;
//import org.jdom2.xpath.XPathExpression;
//import org.jdom2.xpath.XPathFactory;
//import javax.xml.xpath.XPathExpressionException;
//import java.util.Arrays;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.model.MessageDetailVO;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;

//import javax.validation.constraints.NotNull;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.List;

import static net.plumbing.msgbus.common.ClientIpHelper.toCamelCase;
import static net.plumbing.msgbus.threads.utils.MessageUtils.XML_CurrentConfirmation_Tags;
import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;

public class XmlSQLStatement {

    private static final String  TagNameHead       = "SQLRequest";

    private static final String  TagNameSQLStatement  = "SQLStatement";
    private static final String  TagNameTotalStatement  = "TotalStatement";
    private static final String  AttrNameStateType  = "type";
    private static final String  AttrNameStateNum   = "snum";
    private static final String  TagNamePSTMT      = "PSTMT";
    private static final String  TagNameParam      = "Param";
    private static final String  AttrNameParamNum  = "pnum";
    private static final String  AttrNameParamType = "type";
    private static final String  AttrNameParamDir  = "dir";
/*
    public static final String  TagNameResultSet  = "ResultSet";
    public static final String  TagNameResult     = "Result";
    public static final String  AttrNameResNum    = "rnum";
    public static final String  TagNameReturn     = "Return";
    public static final String  TagNameRetNorm    = "Normal";
    public static final String  TagNameRetFault   = "Fault";

    public static final String  ParTypeString   = "string";
    public static final String  ParTypeNumber   = "number";
    public static final String  ParTypeDirIN    = "in";
    public static final String  ParTypeDirOUT   = "out";
*/
    private static final String  OperTypeSel     = "select";
//    public static final String  OperTypeIns     = "insert";
//     public static final String  OperTypeUpdt    = "update";
//    public static final String  OperTypeDel     = "delete";

    private static final String  OperTypeRef    = "refcursor";
    private static final String  OperTypePipe    = "pipefunct";
    private static final String  OperTypeFunc    = "function";
    private static final String  RowTag = "Record";

    public static int ExecuteSQLincludedXML(TheadDataAccess theadDataAccess,
                                            boolean isExtSystemAccess,
                                            Connection extSystemDataConnection,
                                            String Passed_Envelope4XSLTPost,
                                            MessageQueueVO messageQueueVO,
                                            MessageDetails messageDetails, boolean isDebugged, Logger MessegeSend_Log) {
        int nn = 0;
        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;
        messageDetails.MsgReason.setLength(0);
       // messageDetails.MsgReason.append("ExecuteSQLinXML is not ready yet! ");
        Connection current_Connection_4_ExecuteSQL;
        if ( ( isExtSystemAccess ) && ( extSystemDataConnection != null ))
            current_Connection_4_ExecuteSQL = extSystemDataConnection;
        else
            current_Connection_4_ExecuteSQL = theadDataAccess.Hermes_Connection;

        PreparedStatement selectStatement = null;
        String SQLcallableStatementExpression=null;
        //final String SQLparamValue;
        String SQLStatement_functionORselect= OperTypeFunc;
        String SQLStatement_ColumnCount="1";

        String  SelectTotalStatement = null;
        int firstStatementParamNum;
        HashMap<Integer, String > SQLparamValues = new HashMap<Integer, String >(); // SQLparamValues.clear();
        // List <String> SQLparamValues= Arrays.asList();

        StringBuffer msg_Reason = new StringBuffer();
         //final
         // boolean isDebugged=    true; // false; //
         int numSQLStatement_founded = 0;

        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            InputStream parsedMessageStream = new ByteArrayInputStream(Passed_Envelope4XSLTPost.getBytes(StandardCharsets.UTF_8));
            Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);
            try {
                // /SQLRequest/SQLStatement
                Element SQLRequest = document.getRootElement();
                List<Element> SQLRequestList = SQLRequest.getChildren();
                // Перебор всех элементов SQLStatement
                for (int sqlRequestListIndex = 0; sqlRequestListIndex < SQLRequestList.size(); sqlRequestListIndex++) {
                    // Ищем элнмент  == SQLStatement
                    Element SQLStatement = SQLRequestList.get(sqlRequestListIndex);
                    // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML:.getRootElement: SQLRequestList.get(" + sqlRequestListIndex + ") SQLStatement.getName()= (" + SQLStatement.getName() + ")");
                    if (SQLStatement.getName().equals( TagNameSQLStatement )) {
                        numSQLStatement_founded = numSQLStatement_founded +1;
                        Attribute SQLStatement_Type = SQLStatement.getAttribute(AttrNameStateType);
                        SQLStatement_functionORselect = SQLStatement_Type.getValue();
                        if ( SQLStatement_functionORselect.equals(OperTypeFunc) ) // || SQLStatement_functionORselect.equals(OperTypeRef) ) // OperTypeRef - способ задания параметров будут те же, что и у  OperTypeFunc
                             firstStatementParamNum = 1;
                        else firstStatementParamNum = 0;

                        Attribute SQLStatement_ReturnColumnCount = SQLStatement.getAttribute(AttrNameStateNum);
                        SQLStatement_ColumnCount = SQLStatement_ReturnColumnCount.getValue();

                        if (isDebugged)
                            MessegeSend_Log.warn("[{}] ExecuteSQLincludedXML:.SQLStatement.getAttribute(SQLStatement_Type)=`{}` =================================", messageQueueVO.getQueue_Id(), SQLStatement_Type.getValue());
                        // SQLStatement_Type.getValue();
                        // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] =============================================================================================");
                        List<Element> SQLStatementParamList = SQLStatement.getChildren();
                        // Перебор всех элементов SQLStatement
                        for ( int sqlStatementParamListIndex = 0; sqlStatementParamListIndex < SQLStatementParamList.size(); sqlStatementParamListIndex++) {
                            Element SQLStatementParam = SQLStatementParamList.get(sqlStatementParamListIndex);
                            // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML: SQLStatementParamList.get(" + sqlStatementParamListIndex + ") SQLStatementParam.getName()= (" + SQLStatementParam.getName() + "), SQLStatementParam.value=(" + SQLStatementParam.getTextTrim() + ")");
                            //if (isDebugged) MessegeSend_Log.warn("-------------------------------------------------------------------------------------------------------");
                            if (SQLStatementParam.getName().equals(TagNamePSTMT))
                            SQLcallableStatementExpression = SQLStatementParam.getText();

                            Attribute SQLStatementParam_Type;
                            if (SQLStatementParam.getName().equals(TagNameParam)) {
                                List <Attribute>  SQLStatementParamAttributes = SQLStatementParam.getAttributes();
                                // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML: SQLStatementParamAttributes.size = " + SQLStatementParamAttributes.size() );

                                for (int j = 0; j < SQLStatementParamAttributes.size(); j++)
                                {   SQLStatementParam_Type = SQLStatementParamAttributes.get(j);
                                    // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML=> getAttribute: SQLStatementParam_Type.getName(" + SQLStatementParam_Type.getName() + ")=" + SQLStatementParam_Type.getValue() );

                                    if ( SQLStatementParam_Type.getName().equals( AttrNameParamNum )) {
                                        // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML-> SQLStatementParam.getName(" + SQLStatementParam.getName() + "), " + "SQLStatementParam.value=(" + SQLStatementParam.getTextTrim() + ")," + " getAttribute: SQLStatementParam_Type.getName(" + SQLStatementParam_Type.getName() + ")=" + SQLStatementParam_Type.getValue() );
                                        int NN = Integer.parseInt(SQLStatementParam_Type.getValue());
                                        if ( NN > firstStatementParamNum ) {
                                            SQLparamValues.put(NN-firstStatementParamNum-1, SQLStatementParam.getTextTrim()); //
                                            //String sss = SQLStatementParam.getValue();
                                            // if (isDebugged) MessegeSend_Log.warn(" ExecuteSQLincludedXML-> put(" + (NN-firstStatementParamNum-1) +") [" +SQLStatementParam.getTextTrim() + " ]");
                                            //MessegeSend_Log.warn(" ExecuteSQLincludedXML-> sss-put(" + (NN-firstStatementParamNum-1) +") [" + sss + " ]");
                                        }
                                    }
                                }
                                /*
                                SQLStatementParam_Type = SQLStatement.getAttribute(AttrNameParamNum);
                                if ( SQLStatementParam_Type !=null) {
                                    SQLparamValues.add(SQLStatementParam_Type.getValue() );
                                    MessegeSend_Log.warn(" ExecuteSQLincludedXML.getAttribute: SQLStatementParam.getName()= (" + SQLStatementParam.getName() + "), " + SQLStatementParam_Type.getName() + "=" + SQLStatementParam_Type.getValue() );

                                }
                                */
                            }
                            // if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] -------------------------------------------------------------------------------------------------------");
                        }
                    } // нашли SQLStatement

                    if (SQLStatement.getName().equals( TagNameTotalStatement )) {
                        SelectTotalStatement = SQLStatement.getText();
                    }
                }

                if ( SQLcallableStatementExpression == null )
                {
                    messageDetails.MsgReason.append("ExecuteSQLincludedXML: Не нашли " + "/"+ TagNameHead+ "/"+ TagNameSQLStatement + "/" + TagNamePSTMT + " в результате XSLT прообразования " + Passed_Envelope4XSLTPost);
                    MessegeSend_Log.error("[{}] {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
                    return -2;
                }
/*************************************************************************
                String xpathSQLStatementParamExpression = "/"+ TagNameHead +"/"+ TagNameStatement + "/Param[2]"; ///SQLRequest/SQLStatement/Param[2]
                Integer iMsgStaus = 1233;

                XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathSQLStatementParamExpression, Filters.element());
                Element emtMessage = xpathMessage.evaluateFirst(document);
                if ( emtMessage != null ) {
                    SQLparamValue = emtMessage.getText();
                    messageDetails.MsgReason.append(" Param=" + SQLparamValue );
                }
                else {
                    messageDetails.MsgReason.append("Не нашли " + xpathSQLStatementParamExpression + " в " + Passed_Envelope4XSLTPost);
                    MessegeSend_Log.error(messageDetails.MsgReason.toString());
                    return -2;
                }
      /////////////////////////////**********************************************/

                if (isDebugged) {
                    for ( int k =0; k < SQLparamValues.size(); k++ )
                        MessegeSend_Log.warn("[{}] SQLparamValues.get({} )={}", messageQueueVO.getQueue_Id(), k, SQLparamValues.get(k).toString());
                }
                if (isDebugged)
                    MessegeSend_Log.warn("[{}] =====================================================================================", messageQueueVO.getQueue_Id());
                // todo
               // boolean is_NoConfirmation =
                //        MessageRepositoryHelper.isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation(  messageQueueVO.getOperation_Id(), MessegeSend_Log );
                if ( SQLStatement_functionORselect.equals( OperTypeFunc ) )
                    try {
                        current_Connection_4_ExecuteSQL.clearWarnings();
                        CallableStatement callableStatement;
                        // Step 2.B: Creating JDBC CallableStatement
                        callableStatement = current_Connection_4_ExecuteSQL.prepareCall (SQLcallableStatementExpression);
                        if (isDebugged)
                            MessegeSend_Log.info("[{}] SQLcallableStatementExpression_4_{}:{}", messageQueueVO.getQueue_Id(), OperTypeFunc, SQLcallableStatementExpression);
                        // register OUT parameter
                        if (  isExtSystemAccess ) // Внешний вызов возвращает "0~Message"
                            callableStatement.registerOutParameter(1, Types.VARCHAR);
                        else
                        callableStatement.registerOutParameter(1, Types.INTEGER);
                        for ( int k =0; k < SQLparamValues.size(); k++ ) {
                            if (isDebugged)
                                MessegeSend_Log.warn("[{}] callableStatement.setString: SQLparamValues.get({} )={}",messageQueueVO.getQueue_Id(), k, SQLparamValues.get(k).toString());
                            callableStatement.setString(2 + k, SQLparamValues.get(k));
                        }

                        try {
                            // Step 2.C: Executing CallableStatement
                            callableStatement.executeUpdate();
                            // TODO : try change callableStatement.execute(); => callableStatement.executeUpdate(); for PgRee

                        } catch (SQLException e) {
                            e.printStackTrace();
                            messageDetails.MsgReason.append( (", SQLException callableStatement.execute(`"+ messageQueueVO.getOutQueue_Id() + "`):=" + sStackTrace.strInterruptedException(e)) );
                            MessegeSend_Log.error("[{}] , SQLException callableStatement.execute(`{}`):= {}", messageQueueVO.getQueue_Id(), messageQueueVO.getQueue_Id(), e.getMessage());
                            //MessegeSend_Log.error(messageDetails.MsgReason.toString());
                            SQLWarning warning = callableStatement.getWarnings();

                            while (warning != null) {
                                // System.out.println(warning.getMessage());
                                if (isDebugged)
                                    MessegeSend_Log.warn("[{}] callableStatement.SQLWarning: {}", messageQueueVO.getQueue_Id() , warning.getMessage());
                                warning = warning.getNextWarning();
                            }
                            callableStatement.close();
                            current_Connection_4_ExecuteSQL.rollback();
                            return -3;
                        }
                    /*
                    ResultSetMetaData ResultSetMetaData = callableStatement.getMetaData();
                    int ColumnCount = ResultSetMetaData.getColumnCount();
                    int i;
                    for (i=0; i < ColumnCount; i++ ) {
                        MessegeSend_Log.warn(
                        "ColumnType: " + ResultSetMetaData.getColumnType(i) +
                                "ColumnTypeName: " + ResultSetMetaData.getColumnTypeName(i) +
                                "ColumnClassName: " +  ResultSetMetaData.getColumnClassName( i )
                        );
                    }
                    */
                        // get count and print in console
                        SQLWarning warning = callableStatement.getWarnings();

                        while (warning != null) {
                            // System.out.println(warning.getMessage());
                            if (isDebugged)
                                MessegeSend_Log.warn("[{} ] callableStatement.SQLWarning: {}", messageQueueVO.getQueue_Id(), warning.getMessage());
                            warning = warning.getNextWarning();
                        }
                        // todo было: String countS = callableStatement.getString(1);
                        if (  isExtSystemAccess ) // Внешний вызов возвращает "0~Message"
                        {
                            String callableStatementResult = callableStatement.getString(1); // Внешняя функция возвращает строку
                            MessegeSend_Log.warn("[{} ] {} callableStatement.getString=`{}`", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, callableStatementResult);
                            callableStatement.close();
                            current_Connection_4_ExecuteSQL.commit();
                            // - Формируем Confirmation из пришедшей стороки
                            String callStatement_Message;
                            Integer callStatementResult = 0;
                            if ( callableStatementResult != null ) {
                                String[] callableStatementResults = callableStatementResult.split("~");
                                callStatementResult = Integer.parseInt(callableStatementResults[0]);
                                if (callableStatementResults.length > 1) {
                                    if (callableStatementResults[1] != null)
                                        callStatement_Message = callableStatementResults[1];
                                    else callStatement_Message = "`пустое текстовое сообшение`";
                                } else
                                    callStatement_Message = "`нет текстового сообшения`";
                            } else {
                                callStatementResult = 12821;
                                callStatement_Message = "`неожиданно пусто`";
                            }
                            if (isDebugged)
                                MessegeSend_Log.warn("[{}] callStatementResult={} ; callableStatement_MessageResult =`{}`", messageQueueVO.getQueue_Id(),
                                                        callStatementResult, StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(callStatement_Message)));
                                // Формируем псевдо XML_ClearBodyResponse из function, с учетом non-XML символов
                            MakeConfirmation4Function( callStatementResult, "Функция была успешно вызвана,callableStatement_MessageResult =" ,
                                                         StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(callStatement_Message)), messageDetails);
                            // Устанавливаеи признак завершения работы прикладного обработчика  == "EXEIN" , для функции из внешей БД
                            int result = theadDataAccess.doUPDATE_MessageQueue_IN2ExeIN(messageQueueVO.getQueue_Id(),
                                    msg_Reason.toString(),  MessegeSend_Log);
                             messageQueueVO.setQueue_Direction(XMLchars.DirectEXEIN);
                        }
                        else {
                            Integer callableStatementResult = callableStatement.getInt(1);
                            MessegeSend_Log.warn("[{} ] {} callableStatement.getInt={}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, callableStatementResult.toString());
                            callableStatement.close();
                            current_Connection_4_ExecuteSQL.commit();
                        }

                    } catch (SQLException e) {
                        messageDetails.MsgReason.append(OperTypeFunc +  " catch SQLException Connection.prepareCall :=" ); messageDetails.MsgReason.append( e.getMessage() ); //sStackTrace.strInterruptedException(e) );
                        MessegeSend_Log.error("[{}] 328:`{}` {}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, messageDetails.MsgReason.toString());
                        return -2;
                    }
                if ( SQLStatement_functionORselect.equals( OperTypeRef ) )
                    try {
                        // isDebugged = true;
                        CallableStatement callableStatement;
                        // Step 2.B: Creating JDBC CallableStatement
                        callableStatement =  current_Connection_4_ExecuteSQL.prepareCall (SQLcallableStatementExpression);
                        if (isDebugged)
                            MessegeSend_Log.info("[{}] SQLcallableStatementExpression_4_{}:{}", messageQueueVO.getQueue_Id(), OperTypeRef, SQLcallableStatementExpression);
                        // register OUT parameter
                        callableStatement.registerOutParameter(1, Types.REF_CURSOR); // OracleTypes.CURSOR);
                        for ( int k =1; k < SQLparamValues.size(); k++ ) {
                            if (isDebugged)
                                MessegeSend_Log.warn("[{}] SQLparamValues.get({})-> [{}]={}", messageQueueVO.getQueue_Id(), k, 1 + k, SQLparamValues.get(k));
                            callableStatement.setString(( 1 + k), SQLparamValues.get(k));
                        }
                        if (isDebugged)
                        MessegeSend_Log.warn("[{}] SQLparamValues.set All", messageQueueVO.getQueue_Id() );
                        try {
                            // Step 2.C: Executing CallableStatement
                            callableStatement.execute();
//----------------------------------------------------------------------------------------------------------------/
                            try {
                                // Step 2.C: Executing Select-Statement
                                //ResultSet rs = selectStatement.executeQuery();
                                ResultSetMetaData oraRsMetaDatasmd;

                                    ResultSet rs = (ResultSet) callableStatement.getObject(1); // .getResultSet(); //
                                    // TODO Oracle
                                    //OracleResultSet oraRs = (OracleResultSet) callableStatement.getObject(1);
                                if (theadDataAccess.rdbmsVendor.equalsIgnoreCase("postgresql")) {
                                    PgResultSet oraRs = (PgResultSet) callableStatement.getObject(1);
                                    ResultSetMetaData resultSetMetaData = callableStatement.getMetaData(); // getParameterMetaData(); .getMetaData();
                                    if (isDebugged)
                                    MessegeSend_Log.warn("[{}] resultSetMetaData ={}" , messageQueueVO.getQueue_Id(), resultSetMetaData);
                                     oraRsMetaDatasmd = oraRs.getMetaData();
                                }
                                else {
                                    OracleResultSet oraRs = (OracleResultSet) callableStatement.getObject(1);
                                    ResultSetMetaData resultSetMetaData = callableStatement.getMetaData(); // getParameterMetaData(); .getMetaData();
                                    if (isDebugged)
                                    MessegeSend_Log.warn("[{}] resultSetMetaData ={}" , messageQueueVO.getQueue_Id(), resultSetMetaData);
                                    oraRsMetaDatasmd = oraRs.getMetaData();
                                }
                                if (isDebugged)
                                MessegeSend_Log.warn( "[{}] OraResultSetMetaData ={}", messageQueueVO.getQueue_Id() , oraRsMetaDatasmd ) ;
                                //ParameterMetaData resultSetMetaData = callableStatement.getParameterMetaData();
                                int ColumnCount = oraRsMetaDatasmd.getColumnCount(); // Integer.parseInt(SQLStatement_ColumnCount); //
                                int i;
                                /*isDebugged = false;
                                if (isDebugged)
                                    for (i=1; i < ColumnCount+1; i++ ) {
                                        MessegeSend_Log.warn(
                                                "ColumnType: " + resultSetMetaData.getColumnType(i) +
                                                        " , ColumnName: " + resultSetMetaData.getColumnName(i) +
                                                        " , ColumnTypeName: " + resultSetMetaData.getColumnTypeName(i) +
                                                        " , ColumnLabel: " + resultSetMetaData.getColumnLabel(i)
                                        );
                                    }
                                isDebugged = true;*/
                                int num_Rows4Perform = 0;
                                // Формируем псевдо XML_MsgConfirmation из
                                messageDetails.XML_MsgConfirmation.setLength(0);

                                    messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag // <Confirmation>
                                                    + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
                                                    + "0"
                                                    + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
                                                    + XMLchars.OpenTag + XMLchars.TagDetailList + XMLchars.CloseTag //  <DetailList>
                                            // + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag // не ЗАКРЫВАЕМ  </DetailList>
                                            //+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // не ЗАКРЫВАЕМ </Confirmation>
                                    );
                                    String ColumnLabel;

                                    while (rs.next()) {
                                        //OracleResultSetMetaData

                                        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + RowTag + XMLchars.CloseTag //  <ROW>
                                        );
                                        num_Rows4Perform += 1;
                                        for (i = 1; i < ColumnCount + 1; i++) {
                                            ColumnLabel = toCamelCase( oraRsMetaDatasmd.getColumnLabel(i), "_" ); // "Col_0" + i; // toCamelCase( resultSetMetaData.getColumnLabel(i), "_" );
                                            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag);
                                            messageDetails.XML_MsgConfirmation.append( ColumnLabel); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
                                            messageDetails.XML_MsgConfirmation.append( StringEscapeUtils.escapeXml10(rs.getString(i)) )   ;
                                            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag); messageDetails.XML_MsgConfirmation.append(XMLchars.EndTag);
                                            messageDetails.XML_MsgConfirmation.append( ColumnLabel ); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
                                        }
                                        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + RowTag + XMLchars.CloseTag //  </ROW>
                                        );
                                        num_Rows4Perform += 1;
                                    } // Цикл по выборке

                                    messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag //   </DetailList>
                                            + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // </Confirmation>
                                    );

                                rs.close();
                                if (isDebugged)
                                    MessegeSend_Log.warn( "[{}] {}" , messageQueueVO.getQueue_Id(), messageDetails.XML_MsgConfirmation.toString() );

                            } catch (SQLException e) {
                                //e.printStackTrace();
                                messageDetails.MsgReason.append(", SQLException callableStatement.getObject(`");
                                messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10(SQLcallableStatementExpression) );
                                messageDetails.MsgReason.append("`):="); messageDetails.MsgReason.append( e.getMessage()); //sStackTrace.strInterruptedException(e));
                                MessegeSend_Log.error("[{}] {}" , messageQueueVO.getQueue_Id(),messageDetails.MsgReason.toString());
                                callableStatement.close();
                                return -3;
                            }

//----------------------------------------------------------------------------------------------------------------/
                            current_Connection_4_ExecuteSQL.commit();
                        } catch (SQLException e) {
                            messageDetails.MsgReason.append(", [" + messageQueueVO.getOutQueue_Id() + "] SQLException refcursor callableStatement.execute(`"+  SQLcallableStatementExpression + "`):=" + sStackTrace.strInterruptedException(e) );
                            MessegeSend_Log.error("[{}] 445:`{}` {}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, messageDetails.MsgReason.toString());
                            current_Connection_4_ExecuteSQL.rollback();
                            callableStatement.close();
                            return -3;
                        }
                    /*
                    ResultSetMetaData ResultSetMetaData = callableStatement.getMetaData();
                    int ColumnCount = ResultSetMetaData.getColumnCount();
                    int i;
                    for (i=0; i < ColumnCount; i++ ) {
                        MessegeSend_Log.warn(
                        "ColumnType: " + ResultSetMetaData.getColumnType(i) +
                                "ColumnTypeName: " + ResultSetMetaData.getColumnTypeName(i) +
                                "ColumnClassName: " +  ResultSetMetaData.getColumnClassName( i )
                        );
                    }
                    */
                        // get count and print in console
                        // String count = callableStatement.getString(1);
                        int result = theadDataAccess.doUPDATE_MessageQueue_IN2ExeIN(messageQueueVO.getQueue_Id(),
                                msg_Reason.toString(),  MessegeSend_Log);
                        messageQueueVO.setQueue_Direction(XMLchars.DirectEXEIN);
                        callableStatement.close();
                        current_Connection_4_ExecuteSQL.commit();
                    } catch (SQLException e) {
                        messageDetails.MsgReason.append(OperTypeRef +  " SQLException Connection.prepareCall:=" ); messageDetails.MsgReason.append( e.getMessage() ); //sStackTrace.strInterruptedException(e) );
                        MessegeSend_Log.error("[{}] 469:`{}` {}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, messageDetails.MsgReason.toString());
                        return -2;
                    }

                if (( SQLStatement_functionORselect.equals( OperTypeSel ) ) || ( SQLStatement_functionORselect.equals( OperTypePipe))) // либо SELECT из таблиц, либо SELECT * FROM TABLE(do_pipe())
                    try {
                        // Step 2.B: Creating JDBC selectStatement одинково для обоих случаев
                        selectStatement = current_Connection_4_ExecuteSQL.prepareStatement (SQLcallableStatementExpression);
                        msg_Reason.append( SQLcallableStatementExpression);
                        if (isDebugged)
                        MessegeSend_Log.info( SQLcallableStatementExpression );
                        // register OUT parameter
                        if ( SQLparamValues.size() > 0 )
                        msg_Reason.append( " using: " );

                        for ( int k =0; k < SQLparamValues.size(); k++ ) {
                            if (isDebugged)
                            MessegeSend_Log.warn("[{}] selectStatement.setString: SQLparamValues.get({} )={}", messageQueueVO.getQueue_Id(), (1+k), SQLparamValues.get(k));
                            selectStatement.setString(1 + k, SQLparamValues.get(k));
                            msg_Reason.append( SQLparamValues.get(k)); msg_Reason.append( ", " );
                        }

                        try {
                            // Step 2.C: Executing Select-Statement
                            ResultSet rs = selectStatement.executeQuery();
                            ResultSetMetaData ResultSetMetaData = selectStatement.getMetaData();
                            int ColumnCount = ResultSetMetaData.getColumnCount();
                            int i;
                            if (isDebugged)
                            for (i=1; i < ColumnCount+1; i++ ) {
                                MessegeSend_Log.warn("[{}] ColumnType: {} , ColumnName: {} , ColumnTypeName: {} , ColumnLabel: {}", messageQueueVO.getQueue_Id(),
                                        ResultSetMetaData.getColumnType(i), ResultSetMetaData.getColumnName(i), ResultSetMetaData.getColumnTypeName(i),
                                        ResultSetMetaData.getColumnLabel(i));
                            }
                            int num_Rows4Perform = 0;
                            // Формируем псевдо XML_MsgConfirmation из PIPEfunction
                            messageDetails.XML_MsgConfirmation.setLength(0);
                            if ( SQLStatement_functionORselect.equals( OperTypePipe ) ) // формируем из подготовленной структуры TAG_NUM TAG_ID, TAG_VALUE TAG_PAR_NUM
                            MakeConfirmation4PIPEfunction(rs, messageQueueVO.getQueue_Id(), messageDetails, MessegeSend_Log);
                            if ( SQLStatement_functionORselect.equals( OperTypeSel ) ) // именно SELECT из таблиц когда нет подготовленной структуры TAG_NUM TAG_ID, TAG_VALUE TAG_PAR_NUM
                            {

                                messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag // <Confirmation>
                                                + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
                                                + "0"
                                                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
                                                + XMLchars.OpenTag + XMLchars.TagDetailList + XMLchars.CloseTag //  <DetailList>
                                        // + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag // не ЗАКРЫВАЕМ  </DetailList>
                                        //+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // не ЗАКРЫВАЕМ </Confirmation>
                                );
                                String ColumnLabel;
                                while (rs.next()) {
                                    messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + RowTag + XMLchars.CloseTag //  <ROW>
                                    );
                                    num_Rows4Perform += 1;
                                    for (i = 1; i < ColumnCount + 1; i++) {
                                        ColumnLabel = toCamelCase( ResultSetMetaData.getColumnLabel(i), "_" );
                                        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag);
                                        messageDetails.XML_MsgConfirmation.append( ColumnLabel); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
                                        messageDetails.XML_MsgConfirmation.append( StringEscapeUtils.escapeXml10(rs.getString(i)) )   ;
                                        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag); messageDetails.XML_MsgConfirmation.append(XMLchars.EndTag);
                                        messageDetails.XML_MsgConfirmation.append( ColumnLabel ); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
                                    }
                                    messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + RowTag + XMLchars.CloseTag //  </ROW>
                                    );
                                    num_Rows4Perform += 1;
                                } // Цикл по выборке

                                messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag //   </DetailList>
                                        + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // </Confirmation>
                                );
                            }
                            rs.close();
                            if (isDebugged)
                             MessegeSend_Log.warn( messageDetails.XML_MsgConfirmation.toString() );
                            if (isDebugged)
                                MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] num_Rows4Perform=" + num_Rows4Perform );

                        } catch (SQLException e) {
                            //e.printStackTrace();
                            messageDetails.MsgReason.append(", SQLException selectStatement.executeQuery(`");
                            messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10(SQLcallableStatementExpression) );
                            messageDetails.MsgReason.append("`):="); messageDetails.MsgReason.append( e.getMessage()); //sStackTrace.strInterruptedException(e));
                            MessegeSend_Log.error("[{}] {}",messageQueueVO.getQueue_Id(),messageDetails.MsgReason.toString());
                            selectStatement.close();
                            current_Connection_4_ExecuteSQL.rollback();
                            return -3;
                        }

                        // get count and print in console
                        selectStatement.close();
                        // Устанавливаеи признак завершения работы прикладного обработчика  == "EXEIN"
                        int result = theadDataAccess.doUPDATE_MessageQueue_IN2ExeIN(messageQueueVO.getQueue_Id(),
                                                                                    msg_Reason.toString(),  MessegeSend_Log);
                        messageQueueVO.setQueue_Direction(XMLchars.DirectEXEIN);
                        current_Connection_4_ExecuteSQL.commit();
                    } catch (SQLException e) {
                        messageDetails.MsgReason.append( OperTypeSel + " SQLException Hermes_Connection.prepareCall:=`"); messageDetails.MsgReason.append( SQLcallableStatementExpression); messageDetails.MsgReason.append("`"); messageDetails.MsgReason.append( sStackTrace.strInterruptedException(e) );
                        current_Connection_4_ExecuteSQL.rollback();
                        MessegeSend_Log.error("[{}] {}",messageQueueVO.getQueue_Id(),messageDetails.MsgReason.toString());
                        return -2;
                    }
                ////------------- SelectTotalStatement ----------------
                if ( SelectTotalStatement != null )
                {
                        try { selectStatement = current_Connection_4_ExecuteSQL.prepareStatement (SelectTotalStatement);
                            msg_Reason.append( SelectTotalStatement);
                            if (isDebugged)
                                MessegeSend_Log.info("[{}] SelectTotalStatement: {}", messageQueueVO.getQueue_Id(), SelectTotalStatement);
                               ResultSet rs = selectStatement.executeQuery();
                            while (rs.next()) {
                                messageDetails.X_Total_Count = rs.getInt(1);
                            }
                            if (isDebugged)
                                MessegeSend_Log.info("[{}] SelectTotalStatement: X_Total_Count= {}", messageQueueVO.getQueue_Id(), messageDetails.X_Total_Count);
                               rs.close();
                            selectStatement.close();

                    } catch (SQLException e) {

                        messageDetails.MsgReason.append( OperTypeSel +  " SelectTotal SQLException Hermes_Connection.prepareCall(`");
                        messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10(SelectTotalStatement)); messageDetails.MsgReason.append("`" );
                        messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(e)) );
                        MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] "+ messageDetails.MsgReason.toString());
                            selectStatement.close();
                            current_Connection_4_ExecuteSQL.rollback();
                        return -2;
                    }
                }

            } catch (Exception ex) {
                // ex.printStackTrace(System.err);
                messageDetails.MsgReason.setLength(0);
                messageDetails.MsgReason.append("ExecuteSQLincludedXML.XPathFactory.xpath.evaluateFirst fault: "); messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(ex)) );
                MessegeSend_Log.error("[{}] {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
                return -1;
            }
        }catch (JDOMException | IOException ex) {
            //ex.printStackTrace(System.err);
            messageDetails.MsgReason.setLength(0);
            messageDetails.MsgReason.append("ExecuteSQLincludedXML.documentBuilder fault: "); messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(ex)) );
            MessegeSend_Log.error("[{}] {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
            return -4;
        }

/******************** ПОТОМ
        try {
            theadDataAccess.stmtMsgQueueDet.setLong(1, messageQueueVO.getQueue_Id());
            ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                messageDetailVO.setMessageQueue(
                        rs.getString("Tag_Id"),
                        rs.getString("Tag_Value"),
                        rs.getInt("Tag_Num"),
                        rs.getInt("Tag_Par_Num")
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
                messageDetails.MessageRowNum += 1;
                // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");

            }
        } catch (SQLException e) {
            MessegeSend_Log.error("Queue_Id=[" + messageQueueVO.getQueue_Id() + "] :" + sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            return nn;
        }
************************************************************/
        return nn;
    }

    private static int MakeConfirmation4PIPEfunction(ResultSet rs, long Queue_Id, MessageDetails messageDetails,  Logger MessegeReceive_Log) {

        messageDetails.Confirmation.clear();
        messageDetails.ConfirmationRowNum = 0;
        messageDetails.Confirmation_Tag_Num = 0;
        messageDetails.XML_MsgConfirmation.setLength(0);
        messageDetails.XML_MsgConfirmation.trimToSize();

        try {
            String rTag_Value=null;
            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                rTag_Value = StringEscapeUtils.escapeXml10(rs.getString("Tag_Value") );
//                MessegeReceive_Log.warn("_MakeConfirmation4PIPEfunction messageChildVO.Tag_Par_Num=" + rs.getInt("Tag_Par_Num") +
//                        ", messageChildVO.Tag_Num=" + rs.getInt("Tag_Num") +
//                        ", messageChildVO.Tag_Id=" + rs.getString("Tag_Id") +
//                        ", messageChildVO.Tag_Value=" + rTag_Value
//                );
                if ( rTag_Value == null )
                    messageDetailVO.setMessageQueue(
                            rs.getString("Tag_Id"),
                            null,
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                else
                    messageDetailVO.setMessageQueue(
                            rs.getString("Tag_Id"),
                            StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
                messageDetails.ConfirmationRowNum += 1;
                 // MessegeReceive_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
            }
            rs.close();
        } catch (SQLException e) {
            MessegeReceive_Log.error("Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            System.err.println("Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            return -2;
        }
        if ( messageDetails.ConfirmationRowNum > 0 )
            XML_CurrentConfirmation_Tags(messageDetails, 0, MessegeReceive_Log);
        if (  messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeReceive_Log.info("["+ Queue_Id +"] MsgConfirmation: " +  messageDetails.XML_MsgConfirmation.toString());
        return messageDetails.ConfirmationRowNum;
    }

    public static void MakeConfirmation4Function(Integer callableStatementResult, String p_Run_String, String p_Message_String, MessageDetails messageDetails) {
        // Формируем псевдо XML_ClearBodyResponse из function
        String s_Message_String;
        if ( p_Message_String !=null ) s_Message_String = p_Message_String;
        else s_Message_String = "";
        messageDetails.XML_MsgConfirmation.setLength(0);
        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag // <Confirmation>
                + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
                + callableStatementResult.toString()
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
                + XMLchars.OpenTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag //  <Message>
                + s_Message_String
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag // </Message>
        );

        messageDetails.XML_MsgConfirmation.append( XMLchars.OpenTag + XMLchars.TagDetailList + XMLchars.CloseTag //  <DetailList>
                + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
                + callableStatementResult.toString()
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
                + XMLchars.OpenTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag //  <Message>
                + p_Run_String + s_Message_String
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultTxt + XMLchars.CloseTag // </Messag
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag // ЗАКРЫВАЕМ  </DetailList>
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag //  ЗАКРЫВАЕМ </Confirmation>
        );
    }
    /*
    {

        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag // <Confirmation>
                        + XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
                        + "0"
                        + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
                        + XMLchars.OpenTag + XMLchars.TagDetailList + XMLchars.CloseTag //  <DetailList>
                // + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag // не ЗАКРЫВАЕМ  </DetailList>
                //+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // не ЗАКРЫВАЕМ </Confirmation>
        );
        String ColumnLabel;
        while (rs.next()) {
            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + RowTag + XMLchars.CloseTag //  <ROW>
            );
            num_Rows4Perform += 1;
            for (i = 1; i < ColumnCount + 1; i++) {
                ColumnLabel = toCamelCase( ResultSetMetaData.getColumnLabel(i), "_" );
                messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag);
                messageDetails.XML_MsgConfirmation.append( ColumnLabel); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
                messageDetails.XML_MsgConfirmation.append( StringEscapeUtils.escapeXml10(rs.getString(i)) )   ;
                messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag); messageDetails.XML_MsgConfirmation.append(XMLchars.EndTag);
                messageDetails.XML_MsgConfirmation.append( ColumnLabel ); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
            }
            messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + RowTag + XMLchars.CloseTag //  </ROW>
            );
            num_Rows4Perform += 1;
        } // Цикл по выборке

        messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag //   </DetailList>
                + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // </Confirmation>
        );
    }
    */

    public static int ExecuteSQLincludedXML4Send(TheadDataAccess theadDataAccess,
                                                 String Passed_Envelope4XSLTPost,
                                                 MessageQueueVO messageQueueVO,
                                                 @NotNull MessageDetails4Send messageDetails, boolean isDebugged, Logger MessegeSend_Log) {
        int nn = 0;
        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;
        messageDetails.MsgReason.setLength(0);
        // messageDetails.MsgReason.append("ExecuteSQLinXML is not ready yet! ");

        CallableStatement callableStatement;
        final String SQLcallableStatementExpression;
        final String SQLparamValue;


        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            InputStream parsedMessageStream = new ByteArrayInputStream(Passed_Envelope4XSLTPost.getBytes(StandardCharsets.UTF_8));
            Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);

            try {

                String xpathSQLStatementExpression="/SQLRequest/SQLStatement/PSTMT";
                String xpathSQLStatementParamExpression = "/SQLRequest/SQLStatement/Param[2]"; ///SQLRequest/SQLStatement/Param[2]
                Integer iMsgStaus = 1233;

                XPathExpression<Element> xpathSQLStatement = XPathFactory.instance().compile(xpathSQLStatementExpression, Filters.element());
                Element emtSQLStatement = xpathSQLStatement.evaluateFirst(document);
                if ( emtSQLStatement != null ) {
                    SQLcallableStatementExpression = emtSQLStatement.getText();
                    messageDetails.MsgReason.append("ExecuteSQLincludedXML: SQLStatement=(" + SQLcallableStatementExpression + ")");
                }
                else { messageDetails.MsgReason.append("ExecuteSQLincludedXML: Не нашли " + xpathSQLStatementExpression + " в результате XSLT прообразования " + Passed_Envelope4XSLTPost );
                    MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + " ] " + messageDetails.MsgReason.toString());
                    return -2;

                }

                XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathSQLStatementParamExpression, Filters.element());
                Element emtMessage = xpathMessage.evaluateFirst(document);
                if ( emtMessage != null ) {
                    SQLparamValue = emtMessage.getText();
                    messageDetails.MsgReason.append(" Param=" + SQLparamValue );
                }
                else {
                    messageDetails.MsgReason.append("Не нашли " + xpathSQLStatementParamExpression + " в " + Passed_Envelope4XSLTPost);
                    MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + " ] " + messageDetails.MsgReason.toString());
                    return -2;
                }


                try {
                    theadDataAccess.Hermes_Connection.clearWarnings();
                    // Step 2.B: Creating JDBC CallableStatement
                    callableStatement = theadDataAccess.Hermes_Connection.prepareCall (SQLcallableStatementExpression);

                    MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + " ] " +  SQLcallableStatementExpression );
                    // register OUT parameter
                    callableStatement.registerOutParameter(1, Types.INTEGER);
                    callableStatement.setString(2, SQLparamValue );
                    try {

                        // Step 2.C: Executing CallableStatement
                        callableStatement.execute();
                        // COMMIT! ( Мало ли кто не закоммитил )
                        theadDataAccess.Hermes_Connection.commit();
                    } catch (SQLException e) {
                        messageDetails.MsgReason.append(", SQLException callableStatement.execute():=" + e.toString());
                        MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + " ] " + messageDetails.MsgReason.toString());
                        callableStatement.close();
                        theadDataAccess.Hermes_Connection.rollback();
                        return -3;
                    }
                    if (isDebugged ) { // получаем отладочную информацию из SQL-function
                        SQLWarning warning = callableStatement.getWarnings();

                        while (warning != null) {
                            // System.out.println(warning.getMessage());
                            MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + " ] callableStatement.SQLWarning: " + warning.getMessage());
                            warning = warning.getNextWarning();
                        }
                    }
                    // get count and print in console
                    int count = callableStatement.getInt(1);
                    callableStatement.close();
                } catch (SQLException e) {
                    messageDetails.MsgReason.append("SQLExceptio Hermes_Connection.prepareCall:=" + e.toString());
                    MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + " ] " + messageDetails.MsgReason.toString());
                    return -2;
                }

            } catch (Exception ex) {
                ex.printStackTrace(System.err);
                messageDetails.MsgReason.setLength(0);
                messageDetails.MsgReason.append("ExecuteSQLincludedXML.XPathFactory.xpath.evaluateFirst fault: " + sStackTrace.strInterruptedException(ex));

                return -1;
            }
        }catch (JDOMException | IOException ex) {
            ex.printStackTrace(System.err);
            messageDetails.MsgReason.setLength(0);
            messageDetails.MsgReason.append("ExecuteSQLincludedXML.documentBuilder fault: " + sStackTrace.strInterruptedException(ex));
        }

/******************** ПОТОМ
 try {
 theadDataAccess.stmtMsgQueueDet.setLong(1, messageQueueVO.getQueue_Id());
 ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
 while (rs.next()) {
 MessageDetailVO messageDetailVO = new MessageDetailVO();
 messageDetailVO.setMessageQueue(
 rs.getString("Tag_Id"),
 rs.getString("Tag_Value"),
 rs.getInt("Tag_Num"),
 rs.getInt("Tag_Par_Num")
 );
 messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
 messageDetails.MessageRowNum += 1;
 // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");

 }
 } catch (SQLException e) {
 MessegeSend_Log.error("Queue_Id=[" + messageQueueVO.getQueue_Id() + "] :" + sStackTrace.strInterruptedException(e));
 e.printStackTrace();
 return nn;
 }
 ************************************************************/
        return nn;
    }
}
