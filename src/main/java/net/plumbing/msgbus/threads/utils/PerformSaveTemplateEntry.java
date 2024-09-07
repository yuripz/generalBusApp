package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.model.MessageTemplateVO;
import org.slf4j.Logger;

import static net.plumbing.msgbus.common.XMLchars.*;

public class PerformSaveTemplateEntry {

    public static final String Conf_Text_Begin= "<?xml version='1.0' encoding='utf-8'?><TemplConfig>";
    public static final String Conf_Text_End="</TemplConfig>";

    private static  int appendStringBuffer( String configContent, StringBuffer pTemplConfig, String configEntry )
    {
        if ( configContent != null) {
            pTemplConfig.append(OpenTag);
            pTemplConfig.append(configEntry);
            pTemplConfig.append(CloseTag);
            pTemplConfig.append(CDATAopen);
            pTemplConfig.append(configContent);
            pTemplConfig.append(CDATAclose);
            pTemplConfig.append(OpenTag);
            pTemplConfig.append(EndTag);
            pTemplConfig.append(configEntry);
            pTemplConfig.append(CloseTag);
            return 1;
        }
        return 0;
    }
    static public String replace_Conf_Text( long Queue_Id, MessageTemplateVO messageTemplateVO, String configEntry, String configContent, Logger MessageSend_Log) throws Exception {
        StringBuffer TemplConfig = new StringBuffer(Conf_Text_Begin);

        MessageSend_Log.info( "["+ Queue_Id +" ] replace_Conf_Text: configEntry=`{}`" ,configEntry // + "\n configContent:" + configContent
        );
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
            default:
                throw  new java.lang.Exception("Unknown \"configEntry:\"=[" + configEntry+ "]");
        }


        MessageSend_Log.warn("["+ Queue_Id +" ] PerformTemplates, пишем в (" + configEntry + ") [" + configContent + "]");
        appendStringBuffer( messageTemplateVO.getHeaderInXSLT(), TemplConfig , "HeaderInXSLT" );
        appendStringBuffer( messageTemplateVO.getWsdlInterface(), TemplConfig , "WsdlInterface"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD1(), TemplConfig , "WsdlXSD1"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD2(), TemplConfig , "WsdlXSD2"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD3(), TemplConfig , "WsdlXSD3"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD4(), TemplConfig , "WsdlXSD4"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD5(), TemplConfig , "WsdlXSD5"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD6(), TemplConfig , "WsdlXSD6"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD7(), TemplConfig , "WsdlXSD7"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD8(), TemplConfig , "WsdlXSD8"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD9(), TemplConfig , "WsdlXSD9"  );


        appendStringBuffer( messageTemplateVO.getMessageXSD(), TemplConfig , "MessageXSD"  );
        appendStringBuffer( messageTemplateVO.getMessageXSLT(), TemplConfig , "MessageXSLT"  );
        appendStringBuffer( messageTemplateVO.getAckXSLT(), TemplConfig , "AckXSLT"  );
        appendStringBuffer( messageTemplateVO.getMsgAnswXSLT(), TemplConfig , "MsgAnswXSLT"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeXSLTPost(), TemplConfig , "EnvelopeXSLTPost"  );
        appendStringBuffer( messageTemplateVO.getConfigExecute(), TemplConfig , "ConfigExecute"  );
        appendStringBuffer( messageTemplateVO.getConfigPostExec(), TemplConfig , "ConfigPostExec"  );
        appendStringBuffer( messageTemplateVO.getHeaderXSLT(), TemplConfig , "HeaderXSLT"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeInXSLT(), TemplConfig , "EnvelopeInXSLT"  );


        appendStringBuffer( messageTemplateVO.getEnvelopeXSLTExt(), TemplConfig , "EnvelopeXSLTExt"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeNS(), TemplConfig , "EnvelopeNS"  );
        appendStringBuffer( messageTemplateVO.getMessageAnswAck(), TemplConfig , "MessageAnswAck"  );
        appendStringBuffer( messageTemplateVO.getMessageAnswMsgXSLT(), TemplConfig , "MessageAnswMsgXSLT"  );

        appendStringBuffer( messageTemplateVO.getMessageAnswerXSD(), TemplConfig , "MessageAnswerXSD"  );

        appendStringBuffer( messageTemplateVO.getAckAnswXSLT(), TemplConfig , "AckAnswXSLT"  );

        appendStringBuffer( messageTemplateVO.getErrTransXSLT(), TemplConfig , "ErrTransXSLT"  );
        appendStringBuffer( messageTemplateVO.getErrTransXSD(), TemplConfig , "ErrTransXSD"  );

        TemplConfig.append( Conf_Text_End );
        return TemplConfig.toString();
    }


    static public String check_Conf_Text( String configEntry) throws Exception {

        switch ( configEntry) {
            case "EnvelopeInXSLT":
               return configEntry;
               // break;

            case "HeaderInXSLT":
                return configEntry;
                // break;
            case "WsdlInterface":
                return configEntry;
                // break;
            case "WsdlXSD1":
                return configEntry;
                // break;
            case "WsdlXSD2":
                return configEntry;
                // break;
            case "WsdlXSD3":
                return configEntry;
                // break;
            case "WsdlXSD4":
                return configEntry;
                // break;
            case "WsdlXSD5":
                return configEntry;
                // break;
            case "WsdlXSD6":
                return configEntry;
                // break;
            case "WsdlXSD7":
                return configEntry;
                // break;
            case "WsdlXSD8":
                return configEntry;
                // break;
            case "WsdlXSD9":
                return configEntry;
                // break;

            case "ConfigExecute":
                return configEntry;
                // break;
            case "MessageXSD":
                return configEntry;
                // break;
            case "HeaderXSLT":
                return configEntry;
                // break;

            case  "ConfigPostExec":
                return configEntry;
                // break;
            case "EnvelopeXSLTPost":
                return configEntry;
                // break;
            case "MsgAnswXSLT":
                return configEntry;
                // break;

            case "MessageXSLT":
                return configEntry;
                // break;

            case "AckXSLT":
                return configEntry;
                // break;

            case "EnvelopeXSLTExt":
                return configEntry;
                // break;

            case "EnvelopeNS":
                return configEntry;
                // break;
            case "MessageAck":
                return configEntry;
                // break;
            case "MessageAnswAck":
                return configEntry;
                // break;
            case "MessageAnswerXSD":
                return configEntry;
                // break;
            case "MessageAnswMsgXSLT":
                return configEntry;
                // break;
            case "AckXSD":
                return configEntry;
                // break;
            case "AckAnswXSLT":
                return configEntry;
                // break;
            case "HeaderXSD":
                return configEntry;
                // break;
            case "ErrTransXSLT":
                return configEntry;
                // break;
            default:
                throw  new java.lang.Exception("Unknown configEntry:=[" + configEntry+ "]");
                //return null;
        }
         //return configEntry;
    }


}

