package net.plumbing.msgbus.init;
import net.plumbing.msgbus.model.MessageTemplateVO;
import org.slf4j.Logger;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/*import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;
*/

public class ConfigMsgTemplates {

    public static int performConfig(MessageTemplateVO messageTemplateVO, Logger AppThead_log) {
        int sucess = 0;
        if ( messageTemplateVO == null ) return -1;
        String parsedConfig = messageTemplateVO.getConf_Text();
        //AppThead_log.info( parsedConfig );
        if ( parsedConfig == null ) return -2;
        if ( parsedConfig.length() == 0 ) return -3;

        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream parsedConfigStream = new ByteArrayInputStream( parsedConfig.getBytes(StandardCharsets.UTF_8));
            Document document = (Document)documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);

            Element  TemplConfig = document.getRootElement();
            List<Element> list = TemplConfig.getChildren();
            // Перебор всех элементов TemplConfig
            for (int i = 0; i < list.size(); i++) {
                Element TemplateElmnt =  list.get(i);

                String configEntry = TemplateElmnt.getName();
                String configContent = TemplateElmnt.getText();
                if ( configContent.length() < 1 ) configContent = null;

                // AppThead_log.info( "configEntry:" + configEntry // + "\n configContent:" + configContent);
                switch ( configEntry) {
                    case "EnvelopeInXSLT":
                        messageTemplateVO.setEnvelopeInXSLT(configContent);
                        break;

                    case "HeaderInXSLT":
                        messageTemplateVO.setHeaderInXSLT(configContent);
                        break;
                    case "WsdlInterface":
                        messageTemplateVO.setWsdlInterface(configContent);
                        break;
                    case "WsdlXSD1":
                        messageTemplateVO.setWsdlXSD1(configContent);
                        break;
                    case "WsdlXSD2":
                        messageTemplateVO.setWsdlXSD2(configContent);
                        break;
                    case "WsdlXSD3":
                        messageTemplateVO.setWsdlXSD3(configContent);
                        break;
                    case "WsdlXSD4":
                        messageTemplateVO.setWsdlXSD4(configContent);
                        break;
                    case "WsdlXSD5":
                        messageTemplateVO.setWsdlXSD5(configContent);
                        break;
                    case "WsdlXSD6":
                        messageTemplateVO.setWsdlXSD6(configContent);
                        break;
                    case "WsdlXSD7":
                        messageTemplateVO.setWsdlXSD7(configContent);
                        break;
                    case "WsdlXSD8":
                        messageTemplateVO.setWsdlXSD8(configContent);
                        break;
                    case "WsdlXSD9":
                        messageTemplateVO.setWsdlXSD9(configContent);
                        break;

                    case "ConfigExecute":
                        messageTemplateVO.setConfigExecute( configContent );
                        break;
                    case "MessageXSD":
                        messageTemplateVO.setMessageXSD( configContent);
                        break;
                    case "HeaderXSLT":
                        messageTemplateVO.setHeaderXSLT( configContent);
                        break;

                    case  "ConfigPostExec":
                        messageTemplateVO.setConfigPostExec( configContent);

                        break;
                    case "EnvelopeXSLTPost":
                        messageTemplateVO.setEnvelopeXSLTPost( configContent);
                        break;

                    case "MsgAnswXSLT":
                        messageTemplateVO.setMsgAnswXSLT( configContent);
                        break;

                    case "MessageXSLT":
                        messageTemplateVO.setMessageXSLT( configContent);
                        break;

                    case "AckXSLT":
                        messageTemplateVO.setAckXSLT( configContent);
                        break;

                    case "EnvelopeXSLTExt":
                        messageTemplateVO.setEnvelopeXSLTExt( configContent);
                        break;

                    case "EnvelopeNS":
                        messageTemplateVO.setEnvelopeNS( configContent);
                        break;
                    case "MessageAck":
                        messageTemplateVO.setMessageAck( configContent);
                        break;
                    case "MessageAnswAck":
                        messageTemplateVO.setMessageAnswAck( configContent);
                        break;
                    case "MessageAnswerXSD":
                        messageTemplateVO.setMessageAnswerXSD( configContent);
                        break;
                    case "MessageAnswMsgXSLT":
                        messageTemplateVO.setMessageAnswMsgXSLT ( configContent);
                        break;

                    case "AckXSD":
                        messageTemplateVO.setAckXSD( configContent);
                        break;
                    case "AckAnswXSLT":
                        messageTemplateVO.setAckAnswXSLT( configContent);
                        break;
                    case "HeaderXSD":
                        messageTemplateVO.setHeaderXSD( configContent);
                        break;
                    case "ErrTransXSLT":
                        messageTemplateVO.setErrTransXSLT( configContent);
                        break;
                }
            }
        } catch ( JDOMException |  IOException ex) {
            ex.printStackTrace(System.out);
        }
    // /TemplConfig/ConfigExecute


        return sucess;
    }
/***
    private static String getConfigExecute(Document document) throws DOMException, XPathExpressionException {
        //System.out.println("Example 1 - Печать всех элементов Cost");
        XPathFactory pathFactory = XPathFactory.newInstance();
        XPath xpath = pathFactory.newXPath();

        // Пример записи XPath
        // Подный путь до элемента

        XPathExpression expr = xpath.compile("//TemplConfig/ConfigExecute");

        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            return (n.getTextContent());
        }
        return null;
    }
**/
}
