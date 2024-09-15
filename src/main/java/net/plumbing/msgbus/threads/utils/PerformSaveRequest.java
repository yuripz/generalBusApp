package net.plumbing.msgbus.threads.utils;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageDetailVO;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.text.StringEscapeUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import static net.plumbing.msgbus.common.XMLchars.*;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import org.xml.sax.SAXParseException;

import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PerformSaveRequest {

    public static int SaveRequestBody_4_MessageQueue(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, String requestContent_4_Save , Logger MessegeReceive_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException, TransformerException, SAXParseException
    {
        int nn = 0;
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(requestContent_4_Save.getBytes(StandardCharsets.UTF_8));
        Document document = null;
        try {
            document = documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        } catch (JDOMParseException JDOMe) {
            MessegeReceive_Log.error("SaveRequestBody_4_MessageQueue: documentBuilder.build (" + requestContent_4_Save + ") fault");
            throw new JDOMParseException(JDOMe.getMessage() + ": SaveRequestBody_4_MessageQueue=(" + requestContent_4_Save + ")", JDOMe);
        }
        Element RootElement  = document.getRootElement();
        // if ( parsedMessage4SEND.length() == 0 ) return -3;
        if ( RootElement == null ) {
            // фиксируем ошибку
            messageDetails.XML_MsgResponse.setLength(0);
            messageDetails.XML_MsgResponse.append( "documentBuilder.build(`" + requestContent_4_Save + "`) fault, document.getRootElement() is null" );
            return -2;
        }

        //MessegeReceive_Log.warn("SaveMessage4Input begin");
        try {

            // Надо переносить пепеинициализацию messageDetails.Message после того, как распарселили новый XML
            //int Tag_Par_Num = 0;
            messageDetails.Message_Tag_Num = 0;
            messageDetails.Message.clear();
            messageDetails.MessageRowNum = 0;

            // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
            MessageUtils.SplitMessage(messageDetails, RootElement, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                    MessegeReceive_Log);

        } catch (NullPointerException  ex) { // | IOException
            ex.printStackTrace(System.err);
            MessegeReceive_Log.error("[" + Queue_Id + "] SaveRequestBody_4_MessageQueue fault:" + ex.getMessage());
            messageDetails.XML_MsgResponse.setLength(0);
            messageDetails.XML_MsgResponse.append( "SaveRequestBody_4_MessageQueue fault:" + ex.getMessage());
            return -22; // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
        }
        //MessegeReceive_Log.warn("SaveMessage4Input SplitMessage complete");

        // Замещаем полученным массивом messageDetails.Message строки в БД
        nn = MessageUtils.ReplaceMessage(theadDataAccess, Queue_Id, messageDetails, MessegeReceive_Log);
        if ( nn >= 0 )
            return 0;
        //MessegeReceive_Log.warn("SaveMessage4Input finish");
        return nn;
    }

    public static int ReplaceConfirmation_4_MessageQueue(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, String requestContent_4_Save , Logger MessegeReceive_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException, TransformerException, SAXParseException {
        int nn = 0;
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(requestContent_4_Save.getBytes(StandardCharsets.UTF_8));
        Document document = null;
        try {
            document = documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        } catch (JDOMParseException JDOMe) {
            MessegeReceive_Log.error("SaveRequestBody_4_MessageQueue: documentBuilder.build (" + requestContent_4_Save + ") fault");
            throw new JDOMParseException(JDOMe.getMessage() + ": SaveRequestBody_4_MessageQueue=(" + requestContent_4_Save + ")", JDOMe);
        }
        Element RootElement = document.getRootElement();
        // if ( parsedMessage4SEND.length() == 0 ) return -3;
        if (RootElement == null) {
            // фиксируем ошибку
            messageDetails.XML_MsgResponse.setLength(0);
            messageDetails.XML_MsgResponse.append("documentBuilder.build(`").append(requestContent_4_Save).append("`) fault, document.getRootElement() is null");
            return -23;
        }

        if (RootElement.getName().equals(XMLchars.TagConfirmation)) //
        {
            //MessegeReceive_Log.warn("SaveMessage4Input begin");
            try {

                messageDetails.Message_Tag_Num = 0;
                nn = theadDataAccess.doDELETE_Message_Confirmation( Queue_Id, MessegeReceive_Log);
                if ( nn < 0 )
                    return -1;

                // получаем МАХ Tag_Num из messageDetails.Message
                try { // получаем  Tag_Num из select max(Tag_Num) + 1  as  Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ?Queue_Id
                    theadDataAccess.stmtMsgLastBodyTag.setLong(1, Queue_Id);
                    theadDataAccess.stmtMsgLastBodyTag.setLong(2, Queue_Id);
                    ResultSet rs = theadDataAccess.stmtMsgLastBodyTag.executeQuery();
                    while (rs.next()) {
                        messageDetails.Message_Tag_Num = rs.getInt("Tag_Num");
                    }
                    rs.close();
                } catch (SQLException e) {
                    MessegeReceive_Log.error("Queue_Id=[" + Queue_Id + "] for {} fault : {}" , theadDataAccess.selectMsgLastBodyTag , sStackTrace.strInterruptedException(e));
                    System.err.println("Queue_Id=[" + Queue_Id + "] fault :" + e.getMessage() );
                    e.printStackTrace();
                    return messageDetails.ConfirmationRowNum;
                }


                messageDetails.Confirmation.clear();

                // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
                // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>

                // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
                MessageUtils.SplitConfirmation(messageDetails, RootElement, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                        MessegeReceive_Log);

                // Замещаем полученным массивом messageDetails.Message строки в БД
                nn = InsertNewConfirmation(theadDataAccess, Queue_Id, messageDetails, MessegeReceive_Log);

            } catch (NullPointerException ex) { // | IOException
                ex.printStackTrace(System.err);
                MessegeReceive_Log.error("[" + Queue_Id + "] ReplaceConfirmation_4_MessageQueue fault:" + ex.getMessage());
                messageDetails.XML_MsgResponse.setLength(0);
                messageDetails.XML_MsgResponse.append("ReplaceConfirmation_4_MessageQueue fault:" + ex.getMessage());
                return -24; // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
            }
        }
        else {
            MessegeReceive_Log.error("[" + Queue_Id + "] ReplaceConfirmation_4_MessageQueue fault: `" + RootElement.getName()+ "` NONT equals(" + XMLchars.TagConfirmation+ ")" );
            messageDetails.XML_MsgResponse.setLength(0);
            messageDetails.XML_MsgResponse.append("ReplaceConfirmation_4_MessageQueue fault: `" + RootElement.getName()+ "` NONT equals(" + XMLchars.TagConfirmation+ ")" );
        }
        //MessegeReceive_Log.warn("SaveMessage4Input SplitMessage complete");

        if ( nn >= 0 )
            return 0;
        //MessegeReceive_Log.warn("SaveMessage4Input finish");
        return nn;
    }


    private static int InsertNewConfirmation(TheadDataAccess theadDataAccess, long Queue_Id, MessageDetails messageDetails, Logger MessegeReceive_Log) {
        int nn = 0;
        int iNumberRecordInConfirmation=0;

        try {
            for ( iNumberRecordInConfirmation = 0; iNumberRecordInConfirmation < messageDetails.Confirmation.size(); iNumberRecordInConfirmation++) {
                MessageDetailVO MessageDetailVO = messageDetails.Confirmation.get( iNumberRecordInConfirmation );
                theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(2, MessageDetailVO.Tag_Id);
                // StringEscapeUtils.unescapeXml(MessageDetailVO.Tag_Value);
                //theadDataAccess.stmt_INSERT_Message_Details.setString(3, MessageDetailVO.Tag_Value);
                theadDataAccess.stmt_INSERT_Message_Details.setString(3, StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                // MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+  MessageDetailVO.Tag_Id +"] :" + StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, MessageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, MessageDetailVO.Tag_Par_Num);
                theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();

                nn = iNumberRecordInConfirmation;
            }
        } catch ( Exception e) {
            MessegeReceive_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+ iNumberRecordInConfirmation +"] :" + sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append( "ReplaceConfirmation [").append( iNumberRecordInConfirmation).append("] ").append( sStackTrace.strInterruptedException(e) );
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return -2;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            return -3;
        }

        MessegeReceive_Log.info(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + nn + " done");
        return nn;
    }


}
