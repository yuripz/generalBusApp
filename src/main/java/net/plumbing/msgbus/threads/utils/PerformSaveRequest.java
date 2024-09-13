package net.plumbing.msgbus.threads.utils;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
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
        nn = MessageUtils.InsertMessageDetail(theadDataAccess, Queue_Id, messageDetails, MessegeReceive_Log);
        //MessegeReceive_Log.warn("SaveMessage4Input finish");
        return nn;
    }

}
