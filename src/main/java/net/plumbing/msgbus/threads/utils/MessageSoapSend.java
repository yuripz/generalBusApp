package net.plumbing.msgbus.threads.utils;

//import ru.hermes.msgbus.ws.client.core.SoapClient;

//import javax.validation.constraints.NotNull;


import com.google.common.xml.XmlEscapers;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.model.MessageDetailVO;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageDetails4Send;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MessageSoapSend {

	// The SOAP server URI
	private String uriSOAPServer;
	// The SOAP connection
	// private static SOAPConnection soapConnection = null;

	// If you want to add namespace to the header, follow this constant
	private static final String PREFIX_NAMESPACE = "ns";
	private static final String NAMESPACE = "http://other.namespace.to.add.to.header";

	public static String getResponseBody(@NotNull MessageDetails4Send messageDetails, Document p_XMLdocument, Logger MessegeSend_Log) throws JDOMParseException, JDOMException, IOException, XPathExpressionException {
		///////////////////////////////////////////
		SAXBuilder documentBuilder;
		InputStream parsedConfigStream;
		Document document = null;
		//  Если прарсинг ответа НЕ прошел, то тут уже псевдо-ответ от обработчика ошибки парсера
		if ( p_XMLdocument == null ) {
			documentBuilder = new SAXBuilder();
			parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
			try {
				document =  documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
			}
			catch ( JDOMParseException e)
			{
				MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgResponse.toString() + ") fault"  );
				throw new JDOMParseException("client.post:getResponseBody=(" + messageDetails.XML_MsgResponse.toString() + ")", e);
			}
		}
		else //  Прарсинг ответа прошел, используем присланное
			document = p_XMLdocument;


		///////////////////////////////////////////

		// SAXBuilder documentBuilder = new SAXBuilder();
		//                /*DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();*/
		//InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
		//Document document = null;


		Element SoapEnvelope = document.getRootElement();
		int XML_MsgResponseLen = messageDetails.XML_MsgResponse.length();
		boolean isSoapBodyFinded = false;
		if ( SoapEnvelope.getName().equals(XMLchars.Envelope) ) {
			// MessegeSend_Log.error("debug HE-5865: SoapEnvelope.getName()= (" + SoapEnvelope.getName() + ")"  );
			// String deftarget = Envelope.getAttributeValue("default", "all");
			List<Element> list = SoapEnvelope.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				// MessegeSend_Log.error("debug HE-5865: SoapElmnt.getName()= (" + SoapElmnt.getName() + ")"  );
				if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
					//MessegeSend_Log.info("client.post:getResponseBody=(" + SoapElmnt.getName());
					isSoapBodyFinded = true;

					// надо подготовить очищенный от ns: содержимое Body.
					messageDetails.Confirmation.clear();
					messageDetails.XML_ClearBodyResponse.setLength(0); messageDetails.XML_ClearBodyResponse.trimToSize(); // messageDetails.XML_ClearBodyResponse= null;
					messageDetails.XML_ClearBodyResponse= new StringBuilder( XML_MsgResponseLen );
					SoapBody2XML_String(messageDetails, SoapElmnt, MessegeSend_Log);
				}
			}

			if ( !isSoapBodyFinded ) {
				MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8) + ")fault"  );
				throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден Element=" + XMLchars.Body);
			}
		} else {

			throw new XPathExpressionException("getResponseBody: в SOAP-ответе("
					+ (messageDetails.XML_MsgResponse.length() > 512 ? messageDetails.XML_MsgResponse.substring(0, 512) : messageDetails.XML_MsgResponse.toString())
					+ "...) не найден RootElement=" + XMLchars.Envelope);
		}

		return null;
	}

	public static int SoapBody2XML_String(@NotNull MessageDetails4Send messageDetails, Element SoapBody, Logger MessegeSend_Log) {
		MessageDetailVO messageDetailVO = messageDetails.Message.get(0);
		int BodyListSize = 0;
		if ( messageDetailVO == null) return BodyListSize; // debug HE-5865:
		// LinkedList<MessageDetailVO> linkedTags = new LinkedList<>();
		// linkedTags.clear();
		if ( messageDetailVO.Tag_Num != 0 ) {
			List<Element> list = SoapBody.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				//MessegeSend_Log.info("client.post:SoapBody2XML_String=(\n" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + "\n");
				// надо подготовить очищенный от ns: содержимое Body.
				messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + SoapElmnt.getName() + XMLchars.CloseTag);
				MessageSoapSend.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessegeSend_Log);
				messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + XMLchars.EndTag + SoapElmnt.getName() + XMLchars.CloseTag);
				// MessegeSend_Log.info("SoapBody2XML_String(XML_ClearBodyResponse):" + messageDetails.XML_ClearBodyResponse.toString());
			}
		}
		return BodyListSize;
	}

	public static int XML_BodyElemets2StringB(MessageDetails4Send messageDetails, Element EntryElement,
											  Logger MessegeSend_Log) {

		int nn = 0;
		// MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
		// MessegeSend_Log.warn("XML_BodyElemets2StringB getValue: `<" + EntryElement.getName() + ">" + EntryElement.getValue() + "</" + EntryElement.getName() + ">`");
		// MessegeSend_Log.warn("XML_BodyElemets2StringB getText: `<" + EntryElement.getName() + ">" + EntryElement.getText() + "</" + EntryElement.getName() + ">`");
		List<Element> Elements = EntryElement.getChildren();
		// Перебор всех элементов
		for (int i = 0; i < Elements.size(); i++) {
			Element XMLelement = Elements.get(i);
			String ElementEntry = XMLelement.getName();
			String ElementContent = XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); // .getValue() вынимает СОДЕРЖАНИЕ всех дочерних элементов!
			// MessegeSend_Log.warn("XML_BodyElemets2StringB getValue: `<" + ElementEntry + ">" + XMLelement.getValue() + "</" + ElementEntry + ">`");
			// MessegeSend_Log.warn("XML_BodyElemets2StringB getText: `<" + ElementEntry + ">" + XMLelement.getText() + "</" + ElementEntry + ">`");
			messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + ElementEntry);
			//MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");

			List<Attribute> ElementAttributes = XMLelement.getAttributes();
			for (int j = 0; j < ElementAttributes.size(); j++) {
				Attribute XMLattribute = ElementAttributes.get(j);

				String AttributeEntry = XMLattribute.getName();
				String AttributeValue = XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());

				messageDetails.XML_ClearBodyResponse.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
				//MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
			}
			messageDetails.XML_ClearBodyResponse.append(XMLchars.CloseTag);

			if ( ElementContent.length() > 0 ) {
				messageDetails.XML_ClearBodyResponse.append(ElementContent);
				// MessegeSend_Log.info("XML_BodyElemets2StringB[" + ElementContent + "]");
			}

			XML_BodyElemets2StringB(messageDetails, XMLelement,
					MessegeSend_Log);
			messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag);
			//MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");

		}
		return nn;
	}


}
