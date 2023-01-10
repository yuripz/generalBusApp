package net.plumbing.msgbus.common;

//import javax.validation.constraints.NotNull;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class XMLchars {
    public static final String WSDLhi="WSDL";
    public static final String XSDhi="XSD";
    public static final String Space=" ";
    public static final String Quote="\"";
    public static final String Equal="=";
    public static final String OpenTag="<";
    public static final String CloseTag=">";
    public static final String EndTag="/";
    public static final String XMLns="xmlns:";
    public static final String CDATAopen="<![CDATA[";
    public static final String CDATAclose="]]>";
    //public static final String Envelope_Begin="<env:Envelope xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">";
    public static final String Envelope_Begin="<env:Envelope xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\" "
    + "xmlns:urn=\"urn:DefaultNamespace\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
    + ">";
    public static final String Envelope_End="</env:Envelope>";
    public static final String Empty_Header="<env:Header/>";
    public static final String Header_Begin="<env:Header>";
    public static final String Header_End="</env:Header>";
    public static final String Body_Begin="<env:Body>";
    public static final String Body_End="</env:Body>";
    public static final String Fault_Client_Begin="<env:Fault><faultcode>env:Client</faultcode><faultstring>";
    public static final String Fault_Server_Begin="<env:Fault><faultcode>env:Server</faultcode><faultstring>";
    public static final String Fault_End="</faultstring></env:Fault>";
    final public static String TagContext         = "Context";
    final public static String TagEventInitiator  = "EventInitiator";
    final public static String TagEventKey        = "EventKey";
    final public static String TagEventSource     = "Source";
    final public static String TagEventDestination= "Destination";
    final public static String TagEventOperationId= "BusOperationId";


    final public static String TagEntryRec    = "Request";
    final public static String TagEntryInit   = "init";
    final public static String TagEntryKey    = "key";
    final public static String TagEntrySrc    = "src";
    final public static String TagEntryDst    = "dst";
    final public static String TagEntryOpId   = "opid";
    final public static String TagOutIdKey    = "outid";

    final public static String HermesMsgDirection_Cod   = "HRMS";
    final public static String xml_xml ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    public static final String TagMsgHeaderEmpty  = "Header_is_empty";

    public static final String Body="Body";
    public static final String Header="Header";

    public static final String NameRootTagContentJsonResponse    = "MsgData";
    public static final String Envelope="Envelope";
    public static final String TagConfirmation="Confirmation";
    public static final String TagDetailList="DetailList";
    public static final String TagNext ="Next";

    public static final String NameTagResult       = "Result";
    public static final String NameTagResultCod    = "Cod";
    public static final String NameTagResultText   = "Text";

    public static final String NameTagFaultResult  = "ResultCode";
    public static final String NameTagFaultTxt     = "Message";
/*
    public static final String NameTagFault        = "Fault";
    public static final String NameTagFaultNs      = "FaultNS";
    public static final String NameTagFaultCode    = "FaultCode";
   */

    final public static String DirectWAITOUT = "WAITOUT";
    final public static String DirectOUT     = "OUT";
    final public static String DirectTEMP     = "TEMP";
    final public static String DirectERROUT  = "ERROUT";
    final public static String DirectSEND    = "SEND";
    final public static String DirectEXEOUT  = "EXEOUT";
    final public static String DirectRESOUT  = "RESOUT";
    final public static String DirectPOSTOUT = "POSTOUT";
    final public static String DirectATTNOUT = "ATTOUT";
    final public static String DirectDELOUT  = "DELOUT";
    final public static String DirectERRIN  = "ERRIN";
    final public static String DirectEXEIN  = "EXEIN";
    final public static String DirectNEWIN  = "NEWIN";
    final public static String DirectIN  = "IN";
    final public static String DirectPOSTIN = "POSTIN";


    public static final String Envelope_noNS_Begin="<Envelope>";
    public static final String Envelope_noNS_End="</Envelope>";
    public static final String EmptyHeader="<Header/>";
    public static final String Header_4BusOperationId_Begin="<Header>\n" +
            "<Context>\n" +
            "  <EventInitiator>GET.HRMS</EventInitiator>\n" +
            "  <EventKey>-1</EventKey>\n" +
            "  <Source>GET.HRMS</Source>\n" +
            "  <Destination>HRMS</Destination>\n" +
            "  <BusOperationId>";
    public static final String Header_4BusOperationId_End="</BusOperationId>\n" +
            " </Context>\n" +
            "</Header>";
    public static final String Header_noNS_Begin="<Header>";
    public static final String Header_noNS_End="</Header>";
    public static final String MsgId_Begin="<MsgId>";
    public static final String MsgId_End="</MsgId>";
    public static final String Body_noNS_Begin="<Body>";
    public static final String Body_noNS_End="</Body>";
    public static final String Parametrs_Begin="<Parametrs>";
    public static final String Parametrs_End="</Parametrs>";
    public static final String QueryString_Begin="<QueryString>";
    public static final String QueryString_End="</QueryString>";
    public static final String Fault_Client_noNS_Begin="<Fault><faultcode>Client</faultcode><faultstring>";
    public static final String Fault_Server_noNS_Begin="<Fault><faultcode>Server</faultcode><faultstring>";
    public static final String Fault_noNS_End="</faultstring></Fault>";


// пробуем унифицировать Fault
    // было
    /*
    public static final String Fault_Client_noNS_Begin_4_Rest="<message>";
    public static final String Fault_Server_noNS_Begin_4_Rest="<message>";
    public static final String Fault_noNS_End_4_Rest="</message>";

    public static final String Fault_Client_Rest_Begin="{\"message\": \"";
    public static final String Fault_Server_Rest_Begin="{\"message\": \"";
    public static final String Fault_Rest_End="\"\n" + "}";
    */
    // стало
    public static final String Fault_Client_noNS_Begin_4_Rest="<Fault><faultcode>Client</faultcode><faultstring>";
    public static final String Fault_Server_noNS_Begin_4_Rest="<Fault><faultcode>Server</faultcode><faultstring>";
    public static final String Fault_noNS_End_4_Rest="</faultstring></Fault>";

    public static final String Fault_Client_Rest_Begin="{\"Fault\": {\n" + "  \"faultcode\": \"Client\",\n" + "  \"faultstring\": \"";
    public static final String Fault_Server_Rest_Begin="{\"Fault\": {\n" + "  \"faultcode\": \"Server\",\n" + "  \"faultstring\": \"";
    public static final String Fault_Rest_End="\"\n" + "}}";

    //public static final String Fault_Client_Rest_Begin="{\"ClientFault\": \"";
    //public static final String Fault_Server_Rest_Begin="{\"ServerFault\": \"";

    public static final String EmptyXSLT_Result ="<?xml version=\"1.0\" encoding=\"utf-8\"?><nan/>";
      public static final String nanXSLT_Result ="<nan/>";

    final public static int MAX_TAG_VALUE_BYTE_SIZE=3966;

    public static byte[] cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE(String s)  {
        byte[] utf8;
        try {
            utf8 = s.getBytes("UTF-8");
        }
        catch ( UnsupportedEncodingException e) {
            utf8 = s.getBytes();
        }
        if (utf8.length <= MAX_TAG_VALUE_BYTE_SIZE) {
            return utf8;
        }
        if ((utf8[MAX_TAG_VALUE_BYTE_SIZE] & 0x80) == 0) {
            // the limit doesn't cut an UTF-8 sequence
            return Arrays.copyOf(utf8, MAX_TAG_VALUE_BYTE_SIZE);
        }
        int i = 0;
        while ((utf8[MAX_TAG_VALUE_BYTE_SIZE-i-1] & 0x80) > 0 && (utf8[MAX_TAG_VALUE_BYTE_SIZE-i-1] & 0x40) == 0) {
            ++i;
        }
        if ((utf8[MAX_TAG_VALUE_BYTE_SIZE-i-1] & 0x80) > 0) {
            // we have to skip the starter UTF-8 byte
            return Arrays.copyOf(utf8, MAX_TAG_VALUE_BYTE_SIZE-i-1);
        } else {
            // we passed all UTF-8 bytes
            return Arrays.copyOf(utf8, MAX_TAG_VALUE_BYTE_SIZE-i);
        }
    }


}
