package net.plumbing.msgbus.threads.utils;

// import com.google.common.xml.XmlEscapers;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.model.*;
import net.sf.saxon.s9api.*;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.jdom2.output.Format;
import org.slf4j.Logger;

//import javax.validation.constraints.NotNull;
import javax.validation.constraints.NotNull;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.XMLConstants;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;

import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.text.StringEscapeUtils;

import org.xml.sax.SAXParseException;

public class XMLutils {

    public static int Soap_XMLDocument2messageQueueVO(Element Header_Context, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException
    {
      int parseResult;
            if ( Header_Context == null  ){
                MessegeSend_Log.error("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.Header + ". Header_Context == null" );
                throw new XPathExpressionException("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext);
            }

            if ( Header_Context.getName().equals(XMLchars.TagContext) )
            {
                parseResult = makeMessageQueueVO_from_ContextElement( Header_Context, messageQueueVO, MessegeSend_Log  );
            }
            else {
                MessegeSend_Log.error("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext );
                throw new XPathExpressionException("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext);
            }

      return parseResult;
    }

    public static int makeMessageQueueVO_from_ContextElement( Element hContext, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException{
        Long EventKey= messageQueueVO.getQueue_Id();
        if ( hContext.getName().equals(XMLchars.TagContext) ) {
            String EventInitiator="";
            String EventSource="";
            Integer EventOperationId=null;


            List<Element> list = hContext.getChildren();
            // Перебор всех элементов Context
            for (int i = 0; i < list.size(); i++) {
                Element XMLelement = (Element) list.get(i);
                String ElementEntry = XMLelement.getName();
                String ElementContent = XMLelement.getText() ;

                switch ( ElementEntry) {
                    case XMLchars.TagEventInitiator:
                        EventInitiator = ElementContent;
                        break;
                    case XMLchars.TagEventSource:
                        EventSource = ElementContent;
                        break;
                    case XMLchars.TagEventOperationId:
                        EventOperationId = Integer.parseInt(ElementContent);
                        break;
                    case XMLchars.TagEventKey:
                        EventKey = Long.parseLong(ElementContent);
                        if ( EventKey.longValue() == -1L) EventKey = messageQueueVO.getQueue_Id();
                        break;
                }
            }
            MessegeSend_Log.warn("[{}] Ищем MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod (`{}`)", EventKey, EventInitiator);

            int MsgDirectionVO_Key =
                    MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventInitiator, MessegeSend_Log );
            if ( MsgDirectionVO_Key >= 0 ) {
                messageQueueVO.setEventInitiator( MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                                                  MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());
            }
            else {
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventInitiator + ") объявлен неизвестый код системы-инициатора " + EventInitiator);
            }

            MsgDirectionVO_Key =
                    MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventSource, MessegeSend_Log );
            if ( MsgDirectionVO_Key >= 0 ) {
                messageQueueVO.setEventInitiator( MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                                                  MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());
                MessegeSend_Log.info("[{}] Нашли [{}] MsgDirection_Id ({}) Subsys_Cod({})", EventKey, MsgDirectionVO_Key, MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(), MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());
            }
            else {
                MessegeSend_Log.error("[{}] getClearRequest: в SOAP-заголовке ({}) объявлен неизвестый код системы-источника {}", EventKey, XMLchars.TagEventSource, EventSource);
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventSource + ") объявлен неизвестый код системы-источника " + EventSource);
            }
            int MessageTypeVO_Key =
                    MessageRepositoryHelper.look4MessageTypeVO_2_Perform(  EventOperationId , MessegeSend_Log);
            if ( MessageTypeVO_Key >= 0 ) {
                messageQueueVO.setMsg_Type( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() );
                messageQueueVO.setMsg_Type_own( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own() );
                messageQueueVO.setOperation_Id( EventOperationId );
                messageQueueVO.setOutQueue_Id( EventKey.toString() );
                messageQueueVO.setMsg_Reason( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() + "() Ok." );

                MessegeSend_Log.info("[{}] Нашли [{}] Msg_Type({}) Msg_Type_ow({})", EventKey, MessageTypeVO_Key, MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type(), MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own());
            }
            else {
                MessegeSend_Log.error("[{}] getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventOperationId + ") объявлен неизвестый № операцмм {}", EventKey, EventOperationId);
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventOperationId + ") объявлен неизвестый № операцмм " + EventOperationId);
            }
        }
        else {
            String errSring = "getClearRequest(makeMessageQueueVO_from_ContextElement): в SOAP-заголовке не найден " + XMLchars.TagContext +  " hContext.getName()=<" +
            hContext.getName() + ">";
            MessegeSend_Log.error("[{}] {}", EventKey,  errSring);
            throw new XPathExpressionException(errSring);
        }
        MessegeSend_Log.debug("[{}] makeMessageQueueVO_from_ContextElement: {}", EventKey, messageQueueVO.toSring());
      return 0;
    }


    public static int Soap_HeaderRequest2messageQueueVO(String Soap_HeaderRequest, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMException, IOException, XPathExpressionException
    {

        int parseResult = 0;
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(Soap_HeaderRequest.getBytes(StandardCharsets.UTF_8));
        Document Soap_HeaderDocument = null;
        try {
            Soap_HeaderDocument =  documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMParseException e)
        {
            MessegeSend_Log.error("[{}] Soap_HeaderRequest2messageQueueVO: documentBuilder.build ({})fault", messageQueueVO.getQueue_Id(),Soap_HeaderRequest);
            throw new JDOMParseException("client.post:Soap_HeaderRequest2messageQueueVO=(" + Soap_HeaderRequest+ ")", e);
        }

        //MessegeSend_Log.info("documentBuilder.build for (" + Soap_HeaderRequest + ")"  );

        Element hContext = Soap_HeaderDocument.getRootElement();
        parseResult = makeMessageQueueVO_from_ContextElement( hContext, messageQueueVO, MessegeSend_Log  );

        MessegeSend_Log.warn("[{}] Soap_HeaderRequest2messageQueueVO: {}", messageQueueVO.getQueue_Id(), messageQueueVO.toSring());

        return parseResult;
    }

    public static String makeClearRequest(MessageDetails messageDetails,
                                          int MessageTemplateVOkey,
                                          // xlstErrorListener XSLTErrorListener,
                                          StringBuilder ConvXMLuseXSLTerr, boolean isDebugged,
                                          Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException,  SAXParseException, SaxonApiException {

        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgInput.getBytes(StandardCharsets.UTF_8));
        Document document = null;
        try {
            document = documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        } catch (JDOMParseException JDOMe) {
            MessegeSend_Log.error("makeClearRequest: documentBuilder.build (" + messageDetails.XML_MsgInput + ")fault");
            throw new JDOMParseException(JDOMe.getMessage() + ": makeClearRequest=(" + messageDetails.XML_MsgInput + ")", JDOMe);
        }

        // 1й проход - очищаем входной XML от Ns:
        // MessegeSend_Log.info("documentBuilder.build for (" + messageDetails.XML_MsgInput + ")"  );
        Element SoapEnvelope = document.getRootElement();
        // MessegeSend_Log.error("debug HE-5865: SoapEnvelope.getName()= (" + SoapEnvelope.getName() + ")"  );
        // надо подготовить очищенный от ns: содержимое Envelope.
        // Это всё в конструкторе messageDetails.Message.clear();messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();

        if (SoapEnvelope.getName().equals(XMLchars.Envelope)) {
            messageDetails.XML_MsgClear.append(XMLchars.Envelope_noNS_Begin);
            SoapBody2XML_String(messageDetails, SoapEnvelope, MessegeSend_Log);
            messageDetails.XML_MsgClear.append(XMLchars.Envelope_noNS_End);
        } else {
            throw new XPathExpressionException("makeClearRequest: в SOAP-запросе не найден RootElement=" + XMLchars.Envelope);
        }
        //  MessegeSend_Log.warn("Парсим, 1й проход: XML_MsgClear= [" + messageDetails.XML_MsgClear.toString() + "]"  );


        if (MessageTemplateVOkey >= 0) {
            String pEnvelopeInXSLT;
            pEnvelopeInXSLT = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT();

            if ((pEnvelopeInXSLT != null) && (!pEnvelopeInXSLT.isEmpty())) {
                //    в интерфейсном шаблоне обозначено преобразование, которое надо исполнить над XML_MsgClear
                Processor xslt30Processor = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_processor();
                XsltCompiler XsltCompiler = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_xsltCompiler();
                Xslt30Transformer Xslt30Transformer = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_xslt30Transformer();

                ConvXMLuseXSLTerr.setLength(0);
                ConvXMLuseXSLTerr.trimToSize();

                messageDetails.XML_MsgConfirmation.append(
                        XMLutils.ConvXMLuseXSLT30(-1L,
                                messageDetails.XML_MsgClear.toString(),
                                xslt30Processor, XsltCompiler, Xslt30Transformer,
                                pEnvelopeInXSLT,
                                messageDetails.MsgReason,
                                ConvXMLuseXSLTerr,
                                MessegeSend_Log,
                                isDebugged
                        ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                );
                if (isDebugged)
                    MessegeSend_Log.info("ProcessInputMessage(makeClearRequest): после XSLT={" + messageDetails.XML_MsgConfirmation.toString() + "}");

                if (messageDetails.XML_MsgConfirmation.toString().equals(XMLchars.nanXSLT_Result)) {
                    if (isDebugged)
                        MessegeSend_Log.error("В результате XSLT преобразования (`" + messageDetails.XML_MsgClear.toString() + "`)получен пустой XML для заголовка сообщения");
                    messageDetails.MsgReason.append("В результате XSLT преобразования очищенного от 'ns:' (`")
                            .append(messageDetails.XML_MsgClear.substring(1, 260))
                            .append("...`)получен пустой XML для заголовка сообщения");
                    throw new SaxonApiException(messageDetails.MsgReason.toString());
                }
                // Всё ок, записываем в  XML_MsgClear результат из  XML_MsgConfirmation
                messageDetails.XML_MsgClear.setLength(0);
                messageDetails.XML_MsgClear.trimToSize();

                messageDetails.XML_MsgClear.append(messageDetails.XML_MsgConfirmation.toString());
                // removing invalid xml characters from input string
                /* изъятие не-XML символов перенесли в получение параметров, что бы не падал парсер xml
                messageDetails.XML_MsgClear =new  StringBuilder(messageDetails.XML_MsgConfirmation.length() );

                char current; // Used to reference the current character.
                    for (int i = 0; i < messageDetails.XML_MsgConfirmation.length(); i++) {
                        current = messageDetails.XML_MsgConfirmation.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
                        if ((current == 0x9) ||
                                (current == 0xA) ||
                                (current == 0xD) ||
                                ((current >= 0x20) && (current <= 0xD7FF)) ||
                                ((current >= 0xE000) && (current <= 0xFFFD)) ||
                                ((current >= 0x10000) && (current <= 0x10FFFF)))
                            messageDetails.XML_MsgClear.append(current);
                    }
               */
                // очищаем использованный XML_MsgConfirmation
                messageDetails.XML_MsgConfirmation.setLength(0);
                messageDetails.XML_MsgConfirmation.trimToSize();
            } else {
                if (isDebugged)
                    MessegeSend_Log.info("ProcessInputMessage(makeClearRequest): EnvelopeInXSLT is NULL");
            }
        }

        // 2й проход - получаем элемент Context из заголовка ( если есть )
        //  MessegeSend_Log.warn("2й проход - получаем элемент Context из заголовка ( если есть ) InputStreamReader to messageDetails.XML_MsgClear[" +  messageDetails.XML_MsgClear + "]");

        try ( InputStream parsedXML_MsgClearStream = new ByteArrayInputStream(messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8)) )
        {
            messageDetails.Input_Clear_XMLDocument  =  documentBuilder.build(parsedXML_MsgClearStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMParseException e)
        {
            MessegeSend_Log.error("documentBuilder.build {" + messageDetails.XML_MsgClear.toString() + "} fault :" + e.getMessage() );
            throw new JDOMParseException("client.post, 2й проход - получаем элемент Context из заголовка ( если есть ) :getClearRequest=(" + messageDetails.XML_MsgClear.toString() + ")", e);
        }

        SoapEnvelope = messageDetails.Input_Clear_XMLDocument.getRootElement();
        boolean isSoapBodyFinded = false;
        List<Element> SoapEnvelopeList = SoapEnvelope.getChildren();
        // Перебор всех элементов Envelope
        for (int i = 0; i < SoapEnvelopeList.size(); i++) {
            Element SoapElmnt = SoapEnvelopeList.get(i);
            if ( isDebugged )
                MessegeSend_Log.warn(" makeClearRequest:.getRootElement: SoapEnvelopeList.get("+ i+ ") SoapElmnt.getName()= (" + SoapElmnt.getName() + ")"  );
            if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
                List<Element> Request_MethodList = SoapElmnt.getChildren();
                if ( Request_MethodList.size() == 1 ) {
                    Element Request_Method = Request_MethodList.get(0);
                    messageDetails.Request_Method = Request_Method;

                    // Формируем XML-тест содержимого Body включая элемент-метод
                    messageDetails.XML_Request_Method.setLength(0); messageDetails.XML_Request_Method.trimToSize();
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + Request_Method.getName() + XMLchars.CloseTag);
                    XML_RequestElemets2StringB( messageDetails, Request_Method, MessegeSend_Log);
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + XMLchars.EndTag + Request_Method.getName() + XMLchars.CloseTag);

                }
                else {
                    MessegeSend_Log.error("makeClearRequest: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size() + " в(" + messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8) + ")"  );
                    throw new XPathExpressionException("makeClearRequest: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size());
                }
                isSoapBodyFinded = true;

            }
            if ( SoapElmnt.getName().equals(XMLchars.Header) ) {
                Element hContext = SoapElmnt.getChild("Context");
                if ( hContext != null) {
                    messageDetails.Input_Header_Context = hContext;
                    // MessegeSend_Log.info("makeClearRequest: в SOAP-запросе внутри <Header> нашли элемент=Context в(" + messageDetails.XML_MsgClear.toString() + ")"  );
                }
                else {
                    messageDetails.Input_Header_Context = null;
                    // MessegeSend_Log.warn("makeClearRequest: в SOAP-запросе внутри <Header> НЕ НАШЛИ элемент=Context в(" + messageDetails.XML_MsgClear.toString() + ")"  );
                }

            }

        }

        if ( !isSoapBodyFinded ) {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgClear + ")fault"  );
            throw new XPathExpressionException("makeClearRequest: в SOAP-запросе не найден Element=" + XMLchars.Body);
        }

        XMLOutputter xmlOutputter = new XMLOutputter();
        xmlOutputter.setFormat(Format.getPrettyFormat());
        messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();
        messageDetails.XML_MsgClear.append(xmlOutputter.outputString(messageDetails.Input_Clear_XMLDocument));
        return messageDetails.XML_MsgClear.toString();
    }

    public static String makeMessageDetailsRestApi(MessageDetails messageDetails, String pEnvelopeInXSLT,
                                                   int MessageTemplateVOkey, StringBuilder ConvXMLuseXSLTerr,
                                           boolean isDebugged,
                                          Logger MessegeSend_Log)
            throws  JDOMException, IOException, XPathExpressionException, SaxonApiException
    {
        SAXBuilder documentBuilder = new SAXBuilder();

        messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();
        messageDetails.XML_MsgClear.append(  messageDetails.XML_MsgInput );
        // -- используем XML_MsgConfirmation как временный буфер, он пока не нужен

        if ((pEnvelopeInXSLT != null) && (!pEnvelopeInXSLT.isEmpty()) &&
                (MessageTemplateVOkey >=0) // нашли шаблон и передали его индекс в массиве
           ) {
            //    в интерфейсном шаблоне обозначено преобразование, которое надо исполнить над XML_MsgClear
            ConvXMLuseXSLTerr.setLength(0);
            ConvXMLuseXSLTerr.trimToSize();
            Processor xslt30Processor = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_processor();
            XsltCompiler XsltCompiler = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_xsltCompiler();
            Xslt30Transformer Xslt30Transformer = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getEnvelopeInXSLT_xslt30Transformer();

            messageDetails.XML_MsgConfirmation.append(
                    XMLutils.ConvXMLuseXSLT30(-1L,
                            messageDetails.XML_MsgClear.toString(),
                            xslt30Processor, XsltCompiler, Xslt30Transformer,
                            pEnvelopeInXSLT,
                            messageDetails.MsgReason,
                            ConvXMLuseXSLTerr,
                            MessegeSend_Log,
                            isDebugged
                    ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
            );
            if (isDebugged)
                MessegeSend_Log.info("ProcessInputMessage(makeClearRequest): после XSLT={" + messageDetails.XML_MsgConfirmation.toString() + "}");

            if (messageDetails.XML_MsgConfirmation.toString().equals(XMLchars.nanXSLT_Result)) {
                messageDetails.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                throw new SaxonApiException(messageDetails.MsgReason.toString());
            }
            // Всё ок, записываем в  XML_MsgClear результат из  XML_MsgConfirmation
            messageDetails.XML_MsgClear.setLength(0);
            messageDetails.XML_MsgClear.trimToSize();
            messageDetails.XML_MsgClear.append(messageDetails.XML_MsgConfirmation.toString());
            // очищаем использованный XML_MsgConfirmation
            messageDetails.XML_MsgConfirmation.setLength(0);
            messageDetails.XML_MsgConfirmation.trimToSize();
        } else {
            if (isDebugged)
                MessegeSend_Log.info("ProcessInputMessage(makeClearRequest): EnvelopeInXSLT is NULL");
        }
        ////////////////////////////////////////////////////////////


       //  MessegeSend_Log.warn("2й проход - получаем элемент Context из заголовка ( если есть ) InputStreamReader to messageDetails.XML_MsgClear[" +  messageDetails.XML_MsgClear + "]");

        try ( InputStream parsedXML_MsgClearStream = new ByteArrayInputStream(messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8)) )
        {
            messageDetails.Input_Clear_XMLDocument  =  documentBuilder.build(parsedXML_MsgClearStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMException e)
        {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgClear.toString() + ")fault"  );
            throw new JDOMParseException("2й проход - получаем элемент Context из заголовка ( если есть ): makeMessageDetailsRestApi=(" + messageDetails.XML_MsgClear.toString() + ")", e);
        }

        Element  SoapEnvelope = messageDetails.Input_Clear_XMLDocument.getRootElement();
        boolean isSoapBodyFinded = false;
        List<Element> SoapEnvelopeList = SoapEnvelope.getChildren();
        // Перебор всех элементов Envelope
        for (int i = 0; i < SoapEnvelopeList.size(); i++) {
            Element SoapElmnt = SoapEnvelopeList.get(i);
            if ( isDebugged )
            MessegeSend_Log.warn(" makeMessageDetailsRestApi:.getRootElement: SoapEnvelopeList.get("+ i+ ") SoapElmnt.getName()= (" + SoapElmnt.getName() + ")"  );
            if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
                List<Element> Request_MethodList = SoapElmnt.getChildren();
                if ( Request_MethodList.size() == 1 ) {
                    Element Request_Method = Request_MethodList.get(0);
                    messageDetails.Request_Method = Request_Method;

                    // Формируем XML-тест содержимого Body включая элемент-метод
                    messageDetails.XML_Request_Method.setLength(0); messageDetails.XML_Request_Method.trimToSize();
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + Request_Method.getName() + XMLchars.CloseTag);
                        XML_RequestElemets2StringB( messageDetails, Request_Method, MessegeSend_Log);
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + XMLchars.EndTag + Request_Method.getName() + XMLchars.CloseTag);

                }
                else {
                    MessegeSend_Log.error("makeMessageDetailsRestApi: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size() + " в(" + messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8) + ")"  );
                    throw new XPathExpressionException("makeMessageDetailsRestApi: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size());
                }
                isSoapBodyFinded = true;

            }
            messageDetails.Input_Header_Context = null;

        }

        if ( !isSoapBodyFinded ) {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgClear + ")fault"  );
            throw new XPathExpressionException("makeClearRequest: в SOAP-запросе не найден Element=" + XMLchars.Body);
        }

        XMLOutputter xmlOutputter = new XMLOutputter();
        xmlOutputter.setFormat(Format.getPrettyFormat());
        messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();
        messageDetails.XML_MsgClear.append(xmlOutputter.outputString(messageDetails.Input_Clear_XMLDocument));
        return messageDetails.XML_MsgClear.toString();
    }


    public static int SoapBody2XML_String(MessageDetails messageDetails, Element SoapEnvelope, Logger MessegeSend_Log) {

        int BodyListSize = 0;

            List<Element> list = SoapEnvelope.getChildren();
            // Перебор всех элементов Envelope
            for (int i = 0; i < list.size(); i++) {
                Element SoapElmnt = (Element) list.get(i);
                //MessegeSend_Log.info("client.post:SoapBody2XML_String=(" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + ")");
                // надо подготовить очищенный от ns: содержимое Body.
                messageDetails.XML_MsgClear.append(XMLchars.OpenTag + SoapElmnt.getName() + XMLchars.CloseTag);
                XMLutils.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessegeSend_Log);
                messageDetails.XML_MsgClear.append(XMLchars.OpenTag + XMLchars.EndTag + SoapElmnt.getName() + XMLchars.CloseTag);
                // MessegeSend_Log.info("SoapBody2XML_String(XML_ClearBodyResponse):" + messageDetails.XML_MsgClear.toString());
            }

        return BodyListSize;

    }


    public static int XML_RequestElemets2StringB(MessageDetails messageDetails, Element EntryElement,
                                              Logger MessegeSend_Log) {
        int nn = 0;
        //MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
        StringBuilder AttributeEntry = new StringBuilder();
        StringBuilder AttributeValue = new StringBuilder();

        List<Element> Elements = EntryElement.getChildren();
        Element XMLelement;
        Attribute XMLattribute;
        StringBuilder ElementEntry= new StringBuilder();
        //StringBuilder ElementContent = new StringBuilder();
        //byte[] ElementContent;

        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
             XMLelement =  Elements.get(i);
            ElementEntry.setLength(0); ElementEntry.trimToSize();
            ElementEntry.append( XMLelement.getName() );

            //ElementContent = XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE ( StringEscapeUtils.escapeXml10(XMLelement.getTextTrim()) );
            String ElementContent = StringEscapeUtils.escapeXml10(XMLchars.cutUTF8String2MAX_TAG_VALUE_BYTE_SIZE (XMLelement.getTextTrim(), MessegeSend_Log ));
            //byte[] Text_BYTE_SIZE = XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE ( TextTrim);
            // String TextTrimCattet_2_SIZE = new  String(Text_BYTE_SIZE, StandardCharsets.UTF_8 );

            //ElementContent.setLength(0);ElementContent.trimToSize();
            //ElementContent.append(StringEscapeUtils.escapeXml10( TextTrimCattet_2_SIZE ));
            //ElementContent.setLength(0); ElementContent.trimToSize();
            //ElementContent.append( StringEscapeUtils.escapeXml10(XMLelement.getTextTrim()) ); // XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"

            messageDetails.XML_Request_Method.append(XMLchars.OpenTag + ElementEntry);
            //MessegeSend_Log.info("XML_MsgClear.appendBEGIB(" + XMLchars.OpenTag + ElementEntry + ")");
            //MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");
            // MessegeSend_Log.info("XML_Body-XMLelement.getText {<" + XMLelement.getText() + ">}" + " <ElementContent>" + ElementContent + "</ElementContent>" );

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                XMLattribute = ElementAttributes.get(j);
                AttributeEntry.setLength(0); AttributeEntry.trimToSize();
                AttributeEntry.append( XMLattribute.getName() );
                AttributeValue.setLength(0); AttributeValue.trimToSize();
                AttributeValue.append(  StringEscapeUtils.escapeXml10( XMLattribute.getValue() ) );  //XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());
                // MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
                messageDetails.XML_Request_Method.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
            }
            messageDetails.XML_Request_Method.append(XMLchars.CloseTag);
           // if ( ElementContent.length > 1000 ) MessegeSend_Log.info("XML_BodyElemets2StringB ElementContent.length =" + ElementContent.length + ";");
            if ( ! ElementContent.isEmpty() ) {
                //String ElementContentS = new String( ElementContent, StandardCharsets.UTF_8 );
               // if ( ElementContent.length > 1000 ) MessegeSend_Log.info("XML_BodyElemets2StringB ElementContentS.length() =" + ElementContentS.length() + ";");
                messageDetails.XML_Request_Method.append(ElementContent); //, StandardCharsets.UTF_8) ); // ElementContent.toString());
                //MessegeSend_Log.info("XML_BodyElemets2StringB-ElementContent[" + ElementContent + "]");
            }

            XML_RequestElemets2StringB(messageDetails, XMLelement,
                    MessegeSend_Log);
            messageDetails.XML_Request_Method.append(XMLchars.OpenTag).append( XMLchars.EndTag).append(ElementEntry).append(XMLchars.CloseTag);
            //MessegeSend_Log.info("XML_MsgClear.appendEND(" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + ")" );
            //MessegeSend_Log.info("XML_MsgClear.length=" +  messageDetails.XML_MsgClear.length() );
            //MessegeSend_Log.info("XML_MsgClear.String=" +  messageDetails.XML_MsgClear.toString() );
            //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");
        }
        return nn;

    }

    public static int XML_BodyElemets2StringB(MessageDetails messageDetails, Element EntryElement,
                                              Logger MessegeSend_Log) {

        int nn = 0;
         //MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
        StringBuilder AttributeEntry = new StringBuilder();
        StringBuilder AttributeValue = new StringBuilder();

        Element XMLelement;
        Attribute XMLattribute;
        StringBuilder ElementEntry= new StringBuilder();
        //StringBuilder ElementContent = new StringBuilder(); cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE
        //byte[] ElementContent;
        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
             XMLelement = (Element) Elements.get(i);
            //String ElementEntry = XMLelement.getName();
            ElementEntry.setLength(0); ElementEntry.trimToSize();
            ElementEntry.append( XMLelement.getName() );

            //String ElementContent = XMLelement.getTextTrim() ; // XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"
            //ElementContent.setLength(0); ElementContent.trimToSize();
            //ElementContent.append( StringEscapeUtils.escapeXml10(XMLelement.getTextTrim()) ); // XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"
            //ElementContent = XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE ( StringEscapeUtils.escapeXml10(XMLelement.getTextTrim()), MessegeSend_Log );
            String ElementContent = StringEscapeUtils.escapeXml10(XMLchars.cutUTF8String2MAX_TAG_VALUE_BYTE_SIZE (XMLelement.getTextTrim(), MessegeSend_Log )) ;

            messageDetails.XML_MsgClear.append(XMLchars.OpenTag).append(ElementEntry);
            //MessegeSend_Log.info("XML_MsgClear.appendBEGIB(" + XMLchars.OpenTag + ElementEntry + ")");
            //MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");
            // MessegeSend_Log.info("XML_Body-XMLelement.getText {<" + XMLelement.getText() + ">}" + " <ElementContent>" + ElementContent + " </ElementContent>" );

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                 XMLattribute = ElementAttributes.get(j);

                //String AttributeEntry = XMLattribute.getName();
                AttributeEntry.setLength(0); AttributeEntry.trimToSize();
                AttributeEntry.append( XMLattribute.getName() );

                //String AttributeValue = XMLattribute.getValue();  //XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());
                AttributeValue.setLength(0); AttributeValue.trimToSize();
                AttributeValue.append(  StringEscapeUtils.escapeXml10( XMLattribute.getValue() ) );  //XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());
                // MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");

                messageDetails.XML_MsgClear.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
                //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
            }
            messageDetails.XML_MsgClear.append(XMLchars.CloseTag);

            //MessegeSend_Log.info("XML_BodyElemets2StringB ElementContent.length =" + ElementContent.length() + ";");
            // if ( ElementContent.length > 1000 ) MessegeSend_Log.info("XML_BodyElemets2StringB ElementContent.length =" + ElementContent.length + ";");
            if ( !ElementContent.isEmpty()  ) {
                //String ElementContentS = new String( ElementContent, StandardCharsets.UTF_8 );
                // if ( ElementContent.length > 1000 ) MessegeSend_Log.info("XML_BodyElemets2StringB ElementContentS.length() =" + ElementContentS.length() + ";");
                messageDetails.XML_Request_Method.append(ElementContent); //, StandardCharsets.UTF_8) ); // ElementContent.toString());

                messageDetails.XML_MsgClear.append(ElementContent);
                //MessegeSend_Log.info("XML_BodyElemets2StringB-ElementContent[" + ElementContent + "]");
            }


            XML_BodyElemets2StringB(messageDetails, XMLelement,
                    MessegeSend_Log);
            messageDetails.XML_MsgClear.append( (XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag) );
            //MessegeSend_Log.info("XML_MsgClear.appendEND(" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + ")" );
            //MessegeSend_Log.info("XML_MsgClear.length=" +  messageDetails.XML_MsgClear.length() );
            //MessegeSend_Log.info("XML_MsgClear.String=" +  messageDetails.XML_MsgClear.toString() );

            //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");

        }
        return nn;
    }


    public static String ConvXMLuseXSLT30(@NotNull Long QueueId, @NotNull String XMLdata_4_Tranform,
                                          @NotNull Processor xslt30Processor, @NotNull XsltCompiler xslt30Compiler,
                                          @NotNull Xslt30Transformer xslt30Transformer,
                                          @NotNull String checkXSLTtext, StringBuilder MsgResult,
                                          StringBuilder ConvXMLuseXSLTerr,
                                          Logger MessageSend_Log, boolean IsDebugged )
            throws SaxonApiException // TransformerException
    { StreamSource xmlStreamSource;
        ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        MsgResult.setLength(0); MsgResult.trimToSize();

        if ( (checkXSLTtext != null) && ( !checkXSLTtext.isEmpty() ) &&
                (xslt30Transformer == null) // проверяем, получилось ли из проверяемого XSLT скомпилировать xslt30Transformer на этапе загрузки
        ) {
            ConvXMLuseXSLTerr.append(" ConvXMLuseXSLT30: length XSLTtext 4 transform is NOT NULL and XSLTtext is NOT Empty, but xslt30Processor/xslt30Transformer == null");
            MessageSend_Log.error("[{}] {}", QueueId, ConvXMLuseXSLTerr );

            MsgResult.append("ConvXMLuseXSLT30:").append(ConvXMLuseXSLTerr);
            return XMLchars.EmptyXSLT_Result;
        }
        if ( (XMLdata_4_Tranform == null) || ( XMLdata_4_Tranform.length() < XMLchars.EmptyXSLT_Result.length() )  ) {
            ConvXMLuseXSLTerr.append(" ConvXMLuseXSLT30: length XMLdata 4 transform is null OR  < ").append(XMLchars.EmptyXSLT_Result.length());
            if ( IsDebugged )
                MessageSend_Log.info("[{}] ConvXMLuseXSLT30: length XMLdata 4 transform is null OR  < {}", QueueId, XMLchars.EmptyXSLT_Result.length());
            return XMLchars.EmptyXSLT_Result ;
        }

        ByteArrayInputStream xmlInputStream=null;
        ByteArrayOutputStream outputByteArrayStream =new ByteArrayOutputStream();
        String stringResult_of_XSLT = XMLchars.EmptyXSLT_Result;


        try {
            xmlInputStream  = new ByteArrayInputStream(XMLdata_4_Tranform.getBytes(StandardCharsets.UTF_8));

        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT30.ByteArrayInputStream Exception" );
            MessageSend_Log.error("[{}] Exception: {}", QueueId, ConvXMLuseXSLTerr);
            MsgResult.append("ConvXMLuseXSLT30:").append(ConvXMLuseXSLTerr);
            return XMLchars.EmptyXSLT_Result ;
        }

        xmlStreamSource = new StreamSource(xmlInputStream);
        try
        {
            if (IsDebugged)
                MessageSend_Log.warn("[{}] ConvXMLuseXSLT30: using XsltLanguageVersion {}", QueueId, xslt30Compiler.getXsltLanguageVersion());
            Serializer outSerializer = xslt30Processor.newSerializer();
            outSerializer.setOutputProperty(Serializer.Property.METHOD, "xml");
            outSerializer.setOutputProperty(Serializer.Property.ENCODING, "utf-8");
            outSerializer.setOutputProperty(Serializer.Property.INDENT, "no");
            outSerializer.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "no");
            outSerializer.setOutputStream(outputByteArrayStream);
            xslt30Transformer.transform( xmlStreamSource, outSerializer);

            stringResult_of_XSLT = outputByteArrayStream.toString();
            if (!stringResult_of_XSLT.isEmpty()) {
                // System.err.println("result != null, stringResult_of_XSLT:" + stringResult_of_XSLT );
                if ((stringResult_of_XSLT.charAt(0) == '{') || (stringResult_of_XSLT.charAt(0) == '[')) {
                    if (IsDebugged)
                        MessageSend_Log.warn("[{}] json transformer.transform(`{}`)", QueueId, stringResult_of_XSLT);
                } else if (stringResult_of_XSLT.length() < XMLchars.EmptyXSLT_Result.length()) {
                    ConvXMLuseXSLTerr.append(" length Xtransformer.transform(`").append(stringResult_of_XSLT).append("`) < ").append(XMLchars.EmptyXSLT_Result.length());
                    if (IsDebugged)
                        MessageSend_Log.warn("[{}] length result transformer.transform(`{}`) < {}", QueueId, stringResult_of_XSLT, XMLchars.EmptyXSLT_Result.length());
                    stringResult_of_XSLT = XMLchars.EmptyXSLT_Result;
                }
            }
            else {
                ConvXMLuseXSLTerr.append(" length xTransformer.transform(`").append(stringResult_of_XSLT).append("`) == 0 ");
                if (IsDebugged)
                    MessageSend_Log.warn("[{}] length xTransformer.transform(`{}`) == 0", QueueId, stringResult_of_XSLT);
                stringResult_of_XSLT = XMLchars.EmptyXSLT_Result;
            }


            /*
                if ( IsDebugged ) {
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + stringResult_of_XSLT);
            }
            */
        }
        catch ( SaxonApiException exp ) {
            ConvXMLuseXSLTerr.append( sStackTrace.strInterruptedException(exp));
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer TransformerException" );
            exp.printStackTrace();
            MessageSend_Log.error("[{}] ConvXMLuseXSLT30.Transformer TransformerException: {}", QueueId, ConvXMLuseXSLTerr);
            if (  !IsDebugged ) {
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML IN ): {}", QueueId, XMLdata_4_Tranform);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XSLT ): {}", QueueId, checkXSLTtext);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML out ): {}", QueueId, stringResult_of_XSLT);
            }
            MessageSend_Log.error("[{}] ConvXMLuseXSLT30.Transformer.Exception: {}", QueueId, ConvXMLuseXSLTerr);
            MsgResult.append( "ConvXMLuseXSLT30.Transformer TransformerException:");  MsgResult.append( ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }

        /*
        try {
            srcxslt = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes("UTF-8")));
        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }
        result = new StreamResult(fOut);
        try
        {
            TransformerFactory XSLTransformerFactory = TransformerFactory.newInstance();
            XSLTransformerFactory.setErrorListener( XSLTErrorListener ); //!!!! java.lang.IllegalArgumentException: ErrorListener !!!
            transformer = XSLTransformerFactory.newTransformer(srcxslt);
            if ( transformer != null) {
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                // TODO OutputKeys.INDENT for ExeL
                // transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                // transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
                transformer.transform(source, result);
            }
            else result = null;

            if ( result != null) {
                resXSLT = fOut.toString();
                // System.err.println("result != null, res:" + res );
                if ( resXSLT.length() < XMLchars.EmptyXSLT_Result.length())
                    resXSLT = XMLchars.EmptyXSLT_Result;
            }
            else {
                resXSLT = XMLchars.EmptyXSLT_Result;
                // System.err.println("result= null, res:" + res );
            }
            try { fOut.close();} catch( IOException IOexc)  { ; }
            if ( IsDebugged ) {
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + resXSLT);
            }
        }
        catch ( TransformerException exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer Exception" );
            exp.printStackTrace();

            if ( !IsDebugged ) {
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML out ): " + resXSLT);
            }
            MessegeSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:"  + ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        */
        return(stringResult_of_XSLT);
    }

    public static String legasyConvXMLuseXSLT(Long QueueId, String XMLdata_4_Tranform,
                                              String XSLTdata, StringBuilder MsgResult, StringBuilder ConvXMLuseXSLTerr,
                                        xlstErrorListener XSLTErrorListener, Logger MessegeSend_Log, boolean IsDebugged )
            throws TransformerException
    { StreamSource source,srcxslt;
        Transformer transformer;
        StreamResult result;
        ByteArrayInputStream xmlInputStream=null;
        ByteArrayOutputStream fOut=new ByteArrayOutputStream();
        String resXSLT=XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        try {
            xmlInputStream  = new ByteArrayInputStream(XMLdata_4_Tranform.getBytes("UTF-8"));

        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        source = new StreamSource(xmlInputStream);
        try {
            srcxslt = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes("UTF-8")));
        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }
        result = new StreamResult(fOut);
        try
        {
            TransformerFactory XSLTransformerFactory = TransformerFactory.newInstance();
            XSLTransformerFactory.setErrorListener( XSLTErrorListener ); //!!!! java.lang.IllegalArgumentException: ErrorListener !!!
            transformer = XSLTransformerFactory.newTransformer(srcxslt);
            if ( transformer != null) {
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                // TODO OutputKeys.INDENT for ExeL
                // transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                // transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
                transformer.transform(source, result);
            }
            else result = null;

            if ( result != null) {
                resXSLT = fOut.toString();
                // System.err.println("result != null, res:" + res );
                if ( resXSLT.length() < XMLchars.EmptyXSLT_Result.length())
                    resXSLT = XMLchars.EmptyXSLT_Result;
            }
            else {
                resXSLT = XMLchars.EmptyXSLT_Result;
                // System.err.println("result= null, res:" + res );
            }
            try { fOut.close();} catch( IOException IOexc)  { ; }
            if ( IsDebugged ) {
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + resXSLT);
            }
        }
        catch ( TransformerException exp ) {
            ConvXMLuseXSLTerr.append(  sStackTrace.strInterruptedException(exp) );
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer Exception" );
            exp.printStackTrace();

            if ( !IsDebugged ) {
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML out ): " + resXSLT);
            }
            MessegeSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:"  + ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(resXSLT);
    }

    public static  boolean TestXMLByXSD(long Queue_Id, String XMLdata_4_Validate, String xsddata, StringBuilder MsgResult,  Logger MessegeSend_Log)// throws Exception
    {
        Validator valid=null;
        StreamSource reqwsdl=null, xsdss = null;
        Schema shm= null;

        try
        { reqwsdl = new StreamSource(new ByteArrayInputStream(XMLdata_4_Validate.getBytes()));
            xsdss = new StreamSource(new ByteArrayInputStream(xsddata.getBytes()));
            shm = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(xsdss);
            valid =shm.newValidator();
            valid.validate(reqwsdl);

        }
        catch ( Exception exp ) {
            MessegeSend_Log.error("Exception: " + exp.getMessage());
            MsgResult.setLength(0);
            MsgResult.append( "["+ Queue_Id  + "] TestXMLByXSD:"  + exp.getMessage() ); //sStackTrace.strInterruptedException(exp) );
            return false;}
        //MessegeSend_Log.info("validateXMLSchema message\n" + XMLdata_4_Validate + "\n is VALID for XSD\n" + xsddata );
        return true;
    }


}
