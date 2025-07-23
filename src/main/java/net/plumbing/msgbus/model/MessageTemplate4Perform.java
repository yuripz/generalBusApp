package net.plumbing.msgbus.model;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.Xslt30Transformer;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MessageTemplate4Perform {
    private int Template_Id;
    private int Interface_Id;
    private int Operation_Id;
    private int Source_Id;
    private String Src_SubCod;
    private int Destin_Id;
    private String Dst_SubCod;
    private String Msg_Type;
    private String Msg_Type_own;
    private String Template_name;
    private String Template_Dir;
    private String Log_Level;
    private String Conf_Text;
    private String LastMaker;
    private String LastDate;

    private String ConfigExecute;
    private String ConfigPostExec;
    private String MessageXSD;
    private String MessageXSLT;
    private String EnvelopeXSLTExt;
    private String EnvelopeInXSLT;
    private String EnvelopeXSLTPost;
    private String EnvelopeNS;
    private String MessageAck;
    private String MsgAnswXSLT;
    private String MessageAnswAck;
    private String MessageAnswerXSD;
    private String MessageAnswMsgXSLT;

    private String AckXSD;
    private String AckXSLT;
    private String AckAnswXSLT;
    private String ErrTransXSLT;
    private String ErrTransXSD;
    private String HeaderXSD;
    private String HeaderXSLT;

    private String PropHost;
    private String PropUser;
    private String PropPswd;
    private String PropUrl;
    private boolean isPreemptive;
    private String PropWebMetod;
    private String PropJavaMethodName;


    private String PropExeMetodExecute;

    private String PropExeMetodPostExec; // PropExeMetodPostExec
    private boolean isPreemptivePostExec;
    private String PropHostPostExec;
    private String PropUserPostExec;
    private String PropPswdPostExec;
    private String PropUrlPostExec;
    private String PropQueryPostExec;
    private String PropEncoding_Out;
    private String PropEncoding_In;
    private Integer PropTimeout_Conn;
    private Integer PropTimeout_Read;
    private boolean isExtSystemAccess =false;
    private boolean isExtSystemAccessPostExec=false;

    private Integer ShortRetryCount;
    private Integer ShortRetryInterval;
    private Integer LongRetryCount;
    private Integer LongRetryInterval;

    private Integer ShortRetryCountPostExec;
    private Integer ShortRetryIntervalPostExec;
    private Integer LongRetryCountPostExec;
    private Integer LongRetryIntervalPostExec;

    private boolean isDebugged=false;
    private String  SOAPAction;

    private  String  PropSearchString ;
    private  String  PropReplacement ;

//    Integer PropShortRetryCount;
//    Integer PropShortRetryInterval;
//    Integer PropLongRetryCount;
//    Integer PropLongRetryInterval;


    private final String  PropNameShortRetryCount  = "shortRetryCount";
    private final String  PropNameShortRetryInterval  = "shortRetryInterval";
    private final String  PropNameLongRetryCount  = "longRetryCount";
    private final String  PropNameLongRetryInterval  = "longRetryInterval";

    private final String  PropNameWebHost  = "host";
    private final String  PropNameWebUser  = "user";
    private final String  PropNameWebPswd  = "pswd";
    private final String  PropNameWebUrl   = "url";
    private final String  PropNameQuery = "queryString";
    private final String  PropNameCharOut  = "encoding_out";
    private final String  PropNameCharIn   = "encoding_in";
    private final String  PropNameConnTimeOut = "timeout_conn";
    private final String  PropNameReadTimeOut = "timeout_read";


    private final String  PropNameExeMetod     = "ExeMetod";
    private final String  PropExtSystemAccess = "extSysDbAccess";
    private final String  PropNameShName       = "script";
    private final String  PropNameParamPref    = "ParamList";
    private final String  PropNameWebMetod     = "WebMetod";
    public final String   WebRestExeMetod      = "web-rest";
    public final String   WebJsonExeMetod      = "web-json";
    public final String  JavaClassExeMetod     = "java-class";
    public final String  JavaMethodName     = "JavaMethodName";

    private final String  PropDebug  = "debug";
    private final String  PropPreemptive = "Preemptive";
    private final String  PropNameSearchString  = "SearchString";
    private final String  ProprNameReplacement  = "Replacement";
    private final String  PropNameSOAPAction_11 = "SOAPAction";
    private final String PropNameSOAPAction_12 = "action";
    public final String SOAP_ACTION_11 = "SOAPAction",
                        SOAP_ACTION_12 = "action=";

    private Logger MessageTemplate4Perform_Log;
    // AckAnswXSLT
    private Processor AckAnswXSLT_processor; // = new Processor(false);
    private XsltCompiler AckAnswXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private Xslt30Transformer AckAnswXSLT_xslt30Transformer;
    // HeaderInXSLT
    private Processor HeaderInXSLT_processor; // = new Processor(false);
    private XsltCompiler HeaderInXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable HeaderInXSLT_xsltStylesheet;
    private Xslt30Transformer HeaderInXSLT_xslt30Transformer;

    private Xslt30Transformer HeaderXSLT_xslt30Transformer;
    private Xslt30Transformer MessageXSLT_xslt30Transformer;
    private Xslt30Transformer MsgAnswXSLT_xslt30Transformer;
    private Xslt30Transformer AckXSLT_xslt30Transformer;
    private Xslt30Transformer EnvelopeXSLTPost_xslt30Transformer;
    private Xslt30Transformer EnvelopeXSLTExt_xslt30Transformer;
    private Xslt30Transformer ErrTransXSLT_xslt30Transformer;

    private Processor ErrTransXSLT_processor; // = new Processor(false);
    private XsltCompiler ErrTransXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private Processor EnvelopeXSLTExt_processor; // = new Processor(false);
    private XsltCompiler EnvelopeXSLTExt_xsltCompiler; // = processor.newXsltCompiler();
    private Processor EnvelopeXSLTPost_processor; // = new Processor(false);
    private XsltCompiler EnvelopeXSLTPost_xsltCompiler; // = processor.newXsltCompiler();
    private Processor AckXSLT_processor; // = new Processor(false);
    private XsltCompiler AckXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private Processor MessageXSLT_processor; // = new Processor(false);
    private XsltCompiler MessageXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private Processor MsgAnswXSLT_processor; // = new Processor(false);
    private XsltCompiler MsgAnswXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private Processor HeaderXSLT_processor; // = new Processor(false);
    private XsltCompiler HeaderXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private String EndPointUrl;
    private String Type_Connection = null;
    public String printMessageTemplate4Perform() {
        return
                "Template_Id:" + Template_Id + ", " +
                "Template_name:" + Template_name + ", " +
                "Operation_Id:" + Operation_Id + ", " +
                "Source_Id:" + Source_Id + ", " +
                "Src_SubCod:" + Src_SubCod + ", " +
                "Msg_Type:" + Msg_Type  + ", " +
        "LastDate :" + LastDate + ", " +
        "ExeMetodExecute:" + PropExeMetodExecute + ", " +
        //"ShortRetryCount:" + ShortRetryCount + ", " +
        "ShortRetryInterval:" + ShortRetryInterval + ", " +
        "LongRetryCount:" + LongRetryCount + ", " +
        "LongRetryInterval:" + LongRetryInterval + ", " +
                "isDebugged:" + isDebugged + ", " +
                "EndPointUrl:" + EndPointUrl + ", " +
                "PropTimeout_Conn:" + PropTimeout_Conn + ", " +
                "PropTimeout_Read:" + PropTimeout_Read + ", " +
                "Type_Connection:" + Type_Connection + ", " +
                "SOAPAction:" + SOAPAction  + ", " +
                        "PostExecExeMetodExecute:" + PropExeMetodPostExec + ", " +
                        "PostExecShortRetryCount:" + ShortRetryCountPostExec + ", " +
                        "PostExecShortRetryInterval:" + ShortRetryIntervalPostExec + ", " +
                        "PostExecLongRetryCount:" + LongRetryCountPostExec + ", " +
                        "getPropJavaMethodName:" + PropJavaMethodName + ", " +
                        "AckXSLT_xslt30Transformer: `" +
                        ( (AckXSLT_xslt30Transformer != null) ? AckXSLT_xslt30Transformer.toString() :"null") + "`"
        ;
    }
    public MessageTemplate4Perform( MessageTemplateVO messageTemplateVO,
                                    Long Queue_Id,
                                    Logger MessageTemplate_Log
                                    ) {
        //if ( messageTemplateVO != null ) {
        this.MessageTemplate4Perform_Log = MessageTemplate_Log;
            this.Template_Id = messageTemplateVO.getTemplate_Id();
            this.Interface_Id = messageTemplateVO.getInterface_Id();
            this.Operation_Id = messageTemplateVO.getOperation_Id();
            this.Source_Id = messageTemplateVO.getSource_Id();
            this.Src_SubCod = messageTemplateVO.getSrc_SubCod();
            this.Destin_Id = messageTemplateVO.getDestin_Id();
            this.Dst_SubCod = messageTemplateVO.getDst_SubCod();
            this.Msg_Type = messageTemplateVO.getMsg_Type();
            this.Msg_Type_own = messageTemplateVO.getMsg_Type_own();
            this.Template_name = messageTemplateVO.getTemplate_name();
            this.Template_Dir = messageTemplateVO.getTemplate_Dir();
            this.Log_Level = messageTemplateVO.getLog_Level();
            this.Conf_Text = messageTemplateVO.getConf_Text();
            this.LastMaker = messageTemplateVO.getLastMaker();
            this.LastDate = messageTemplateVO.getLastDate();


            this.ShortRetryCount = 0;
            this.ShortRetryInterval = 10;
            this.LongRetryCount= 0;
            this.LongRetryInterval = 0;
        this.ShortRetryCountPostExec = ShortRetryCount;
        this.ShortRetryIntervalPostExec = ShortRetryInterval;
        this.LongRetryCountPostExec= LongRetryCount;
        this.LongRetryIntervalPostExec = LongRetryInterval;
            this.HeaderXSLT = messageTemplateVO.getHeaderXSLT();
            this.EnvelopeXSLTExt = messageTemplateVO.getEnvelopeXSLTExt();
            this.AckXSLT = messageTemplateVO.getAckXSLT();
            this.MessageAnswMsgXSLT = messageTemplateVO.getMessageAnswMsgXSLT();
            this.EnvelopeInXSLT = messageTemplateVO.getEnvelopeInXSLT();
            this.AckAnswXSLT = messageTemplateVO.getAckAnswXSLT();
            this.isPreemptivePostExec =false;
            this.isPreemptive =false;

        this.AckAnswXSLT_processor = messageTemplateVO.getAckAnswXSLT_processor();
        this.AckAnswXSLT_xsltCompiler = messageTemplateVO.getAckAnswXSLT_xsltCompiler();
        this.AckAnswXSLT_xslt30Transformer = messageTemplateVO.getAckAnswXSLT_xslt30Transformer();
        // HeaderInXSLT
        this.HeaderInXSLT_processor = messageTemplateVO.getHeaderInXSLT_processor() ; // = new Processor(false);
        this.HeaderInXSLT_xsltCompiler = messageTemplateVO.getHeaderInXSLT_xsltCompiler() ; // = processor.newXsltCompiler();
        this.HeaderInXSLT_xslt30Transformer = messageTemplateVO.getHeaderInXSLT_xslt30Transformer();

        this.HeaderXSLT_xslt30Transformer = messageTemplateVO.getHeaderXSLT_xslt30Transformer();
        this.MessageXSLT_xslt30Transformer = messageTemplateVO.getMessageXSLT_xslt30Transformer();
        this.MsgAnswXSLT_xslt30Transformer = messageTemplateVO.getMsgAnswXSLT_xslt30Transformer();
        this.AckXSLT_xslt30Transformer = messageTemplateVO.getAckXSLT_xslt30Transformer();
        this.EnvelopeXSLTPost_xslt30Transformer = messageTemplateVO.getEnvelopeXSLTPost_xslt30Transformer();
        this.EnvelopeXSLTExt_xslt30Transformer = messageTemplateVO.getEnvelopeXSLTExt_xslt30Transformer();
        this.ErrTransXSLT_xslt30Transformer = messageTemplateVO.getErrTransXSLT_xslt30Transformer();
        this.ErrTransXSLT_processor = messageTemplateVO.getErrTransXSLT_processor() ;
        this.ErrTransXSLT_xsltCompiler = messageTemplateVO.getErrTransXSLT_xsltCompiler()  ;
        this.EnvelopeXSLTExt_processor = messageTemplateVO.getEnvelopeXSLTExt_processor() ;
        this.EnvelopeXSLTExt_xsltCompiler = messageTemplateVO.getEnvelopeXSLTExt_xsltCompiler()  ;
        this.EnvelopeXSLTPost_processor = messageTemplateVO.getEnvelopeXSLTPost_processor() ;
        this.EnvelopeXSLTPost_xsltCompiler = messageTemplateVO.getEnvelopeXSLTPost_xsltCompiler()  ;

        this.AckXSLT_processor = messageTemplateVO.getMAckXSLT_processor() ;
        this.AckXSLT_xsltCompiler = messageTemplateVO.getAckXSLT_xsltCompiler()  ;

        this.MessageXSLT_processor = messageTemplateVO.getMessageXSLT_processor() ;
        this.MessageXSLT_xsltCompiler = messageTemplateVO.getMessageXSLT_xsltCompiler()  ;
        this.MsgAnswXSLT_processor = messageTemplateVO.getMsgAnswXSLT_processor() ;
        this.MsgAnswXSLT_xsltCompiler = messageTemplateVO.getMsgAnswXSLT_xsltCompiler()  ;
        this.HeaderXSLT_processor = messageTemplateVO.getHeaderXSLT_processor() ;
        this.HeaderXSLT_xsltCompiler = messageTemplateVO.getHeaderXSLT_xsltCompiler()  ;

            this.ConfigExecute = messageTemplateVO.getConfigExecute();
            if (this.ConfigExecute != null) {
                Properties properties=new Properties();
                this.isDebugged=false;
                InputStream propertiesStream = new ByteArrayInputStream( this.ConfigExecute.getBytes(StandardCharsets.UTF_8));
                try {
                    properties.load(propertiesStream);
                    //проходимся по всем ключам и печатаем все их значения на консоль

                    for (String key : properties.stringPropertyNames()) {
                         MessageTemplate_Log.info( "[" + Queue_Id + "]" + "ConfigExecute Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                        if ( key.equals(PropNameExeMetod) ) this.PropExeMetodExecute = properties.getProperty(key);
                        if ( key.equals(PropNameShortRetryCount)) this.ShortRetryCount = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameShortRetryInterval)) this.ShortRetryInterval = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameLongRetryCount)) this.LongRetryCount = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameLongRetryInterval)) this.LongRetryInterval = Integer.valueOf(properties.getProperty(key) );

                        if ( key.equals(PropNameWebHost)) this.PropHost = properties.getProperty(key);
                        if ( key.equals(PropNameWebUrl)) this.PropUrl = properties.getProperty(key);
                        if ( key.equals(PropPreemptive) ) {
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("true") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("TRUE") )
                            )
                            {
                                MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropPreemptive Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                                this.isPreemptive=true;
                            }
                        }


                        if ( key.equals(PropNameConnTimeOut)) this.PropTimeout_Conn = Integer.valueOf(properties.getProperty(key));
                        if ( key.equals(PropNameReadTimeOut)) this.PropTimeout_Read = Integer.valueOf(properties.getProperty(key));

                        if ( key.equals(PropNameCharOut)) this.PropEncoding_Out = properties.getProperty(key);
                        if ( key.equals(PropNameCharIn)) this.PropEncoding_In = properties.getProperty(key);

                        if ( key.equals(PropNameWebUser)) this.PropUser = properties.getProperty(key);
                        if ( key.equals(PropNameWebPswd)) this.PropPswd = properties.getProperty(key);

                        if ( key.equals(PropNameWebMetod)) this.PropWebMetod = properties.getProperty(key);
                        if ( key.equals(JavaMethodName)) this.PropJavaMethodName = properties.getProperty(key);
                        if ( key.equals(PropNameSearchString)) this.PropSearchString = properties.getProperty(key).trim();
                        if ( key.equals(ProprNameReplacement)) this.PropReplacement = properties.getProperty(key).trim();
                        if ( key.equals(PropNameSOAPAction_11)) {
                            this.SOAPAction = properties.getProperty(key);
                            MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropNameSOAPAction Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                        }
                        if ( key.equals(PropDebug) ) {
                           //   MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropDebug Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("full") )
                            )
                            {
                             //   MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropDebug Property[" + key +"]=lover[" + properties.getProperty(key) + "]" );
                                this.isDebugged=true;
                            }
                            if (( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("FULL") )
                            )
                            {
                             //   MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropDebug Property[" + key +"]=UPPER[" + properties.getProperty(key) + "]" );
                                this.isDebugged=true;
                            }
                        }
                        if ( key.equals(PropExtSystemAccess) ) {
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("true") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("TRUE") )
                            )
                            {
                                MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropExtSystemAccess Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                                this.isExtSystemAccess=true;
                            }
                        }

                      /*  else {
                            MessageTemplate_Log.info( "[" + Queue_Id + "] (" + key + "( != (" + PropDebug + ")" );
                        }*/
                    }
                }catch ( IOException ex) {
                    ex.printStackTrace(System.out);
                }
            }
            if (( PropHost != null ) && ( PropUrl != null )) {
                if (!PropHost.isEmpty())
                    EndPointUrl = PropHost + PropUrl;
            }
            else EndPointUrl = "http://no.endpoint.error";

            if ( PropTimeout_Conn == null ) PropTimeout_Conn = 10;
            if ( PropTimeout_Read == null ) PropTimeout_Read = 300;

            Type_Connection = "REST";

            this.MessageXSLT = messageTemplateVO.getMessageXSLT();
            this.ConfigPostExec = messageTemplateVO.getConfigPostExec();
            if ( this.ConfigPostExec != null) {
                Properties properties=new Properties();
                InputStream propertiesStream = new ByteArrayInputStream( this.ConfigPostExec.getBytes(StandardCharsets.UTF_8));
                try {
                    properties.load(propertiesStream);

                    //проходимся по всем ключам и печатаем все их значения на консоль
                    for (String key : properties.stringPropertyNames()) {
                        // MessageTemplate_Log.info( "[" + Queue_Id + "]" + "ConfigPostExec Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                        if ( key.equals(PropNameExeMetod)) PropExeMetodPostExec = properties.getProperty(key);
                        if ( key.equals(PropNameWebHost)) PropHostPostExec = properties.getProperty(key);
                        if ( key.equals(PropNameWebUrl)) PropUrlPostExec = properties.getProperty(key);
                        if ( key.equals(PropNameQuery)) PropQueryPostExec  = properties.getProperty(key);
                        if ( key.equals(PropNameWebUser)) PropUserPostExec = properties.getProperty(key);
                        if ( key.equals(PropPreemptive) ) {
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("true") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("TRUE") )
                            )
                            {
                                MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PreemptivePostExec Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                                this.isPreemptivePostExec=true;
                            }
                        }
                        if ( key.equals(PropNameWebPswd)) PropPswdPostExec = properties.getProperty(key);
                        if ( key.equals(PropNameShortRetryCount)) this.ShortRetryCountPostExec = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameShortRetryInterval)) this.ShortRetryIntervalPostExec = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameLongRetryCount)) this.LongRetryCountPostExec = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropNameLongRetryInterval)) this.LongRetryIntervalPostExec = Integer.valueOf(properties.getProperty(key) );
                        if ( key.equals(PropExtSystemAccess) ) {
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("true") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("TRUE") )
                            )
                            {
                                MessageTemplate_Log.info( "[" + Queue_Id + "]" + "PropExtSystemAccess 4 PostExec Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                                this.isExtSystemAccessPostExec=true;
                            }
                        }

                       // if ( key.equals(PropNameConnTimeOut)) PropTimeout_ConnPostExec = Integer.valueOf(properties.getProperty(key));
                      //  if ( key.equals(PropNameReadTimeOut)) PropTimeout_ReadPostExec = Integer.valueOf(properties.getProperty(key));

                       // if ( key.equals(PropNameCharOut)) PropEncoding_OutPostExec = properties.getProperty(key);
                       // if ( key.equals(PropNameCharIn)) PropEncoding_InPostExec = properties.getProperty(key);

                       // if ( key.equals(PropNameWebMetod)) PropWebMetodPostExec = properties.getProperty(key);
                    }
                }catch ( IOException ex) {
                    ex.printStackTrace(System.out);
                }
            }

            this.EnvelopeXSLTPost = messageTemplateVO.getEnvelopeXSLTPost();
            this.MsgAnswXSLT = messageTemplateVO.getMsgAnswXSLT();
            this.MessageXSD = messageTemplateVO.getMessageXSD();
    }

    public Xslt30Transformer getAckAnswXSLT_xslt30Transformer() {return AckAnswXSLT_xslt30Transformer;}
    public Processor getAckAnswXSLT_processor() {return AckAnswXSLT_processor;}
    public XsltCompiler getAckAnswXSLT_xsltCompiler() {return AckAnswXSLT_xsltCompiler;}

    public Xslt30Transformer getEnvelopeXSLTExt_xslt30Transformer() {return EnvelopeXSLTExt_xslt30Transformer;}
    public Xslt30Transformer getEnvelopeXSLTPost_xslt30Transformer() {return EnvelopeXSLTPost_xslt30Transformer;}
    public Xslt30Transformer getAckXSLT_xslt30Transformer() {return AckXSLT_xslt30Transformer;}
    public Xslt30Transformer getMessageXSLT_xslt30Transformer() {return MessageXSLT_xslt30Transformer;}
    public Xslt30Transformer getMsgAnswXSLT_xslt30Transformer() {return MsgAnswXSLT_xslt30Transformer;}
    public Xslt30Transformer getHeaderXSLT_xslt30Transformer() {return HeaderXSLT_xslt30Transformer;}
    public Xslt30Transformer getErrTransXSLT_xslt30Transformer() {return ErrTransXSLT_xslt30Transformer;}

    public Processor getHeaderXSLT_processor() {return HeaderXSLT_processor;}
    public XsltCompiler getHeaderXSLT_xsltCompiler() {return HeaderXSLT_xsltCompiler;}
    public Processor getMsgAnswXSLT_processor() {return MsgAnswXSLT_processor;}
    public XsltCompiler getMsgAnswXSLT_xsltCompiler() {return MsgAnswXSLT_xsltCompiler;}
    public Processor getMessageXSLT_processor() {return MessageXSLT_processor;}
    public XsltCompiler getMessageXSLT_xsltCompiler() {return MessageXSLT_xsltCompiler;}
    public Processor getAckXSLT_processor() {return AckXSLT_processor;}
    public XsltCompiler getAckXSLT_xsltCompiler() {return AckXSLT_xsltCompiler;}
    public Processor getEnvelopeXSLTPost_processor() {return EnvelopeXSLTPost_processor;}
    public XsltCompiler getEnvelopeXSLTPost_xsltCompiler() {return EnvelopeXSLTPost_xsltCompiler;}
    public Processor getEnvelopeXSLTExt_processor() {return EnvelopeXSLTExt_processor;}
    public XsltCompiler getEnvelopeXSLTExt_xsltCompiler() {return EnvelopeXSLTExt_xsltCompiler;}
    public Processor getErrTransXSLT_processor() {return ErrTransXSLT_processor;}
    public XsltCompiler getErrTransXSLT_xsltCompiler() {return ErrTransXSLT_xsltCompiler;}

    public String getPropJavaMethodName() { return  this.PropJavaMethodName; }
    public  String getEnvelopeXSLTExt() { return this.EnvelopeXSLTExt; }
    public  String getEnvelopeInXSLT() { return this.EnvelopeInXSLT; }
    public  String getPropReplacement() { return this.PropReplacement; }
    public  String getPropSearchString() { return this.PropSearchString; }
    public  String getSOAPAction() { return this. SOAPAction; }
    public  String getEndPointUrl() { return this.EndPointUrl; }
    public boolean getIsDebugged() {
        // TODO: this.isDebugged=true; -- для Документирования
        //return true;
        return this.isDebugged;
    }
    public boolean getIsExtSystemAccess() {
        // TODO: this.isExtSystemAccess=true; -- для Документирования
        //return true;
        return this.isExtSystemAccess;
    }
    // isExtSystemAccessPostExec
    public boolean getIsExtSystemAccessPostExec() {
        // TODO: this.isExtSystemAccessPostExec=true; -- для Документирования
        //return true;
        return this.isExtSystemAccessPostExec;
    }
    public  Integer getPropTimeout_Conn() { return  this.PropTimeout_Conn;}
    public  Integer getPropTimeout_Read() { return  this.PropTimeout_Read;}

    public  String getMessageXSLT() { return this.MessageXSLT; }
    public String getMessageXSD () { return this.MessageXSD ; }
    public String getAckXSLT() { return this.AckXSLT ; }
    public String getAckAnswXSLT() { return this.AckAnswXSLT ; }
    public  String getConfigPostExec() { return this.ConfigPostExec; }
    public  String getEnvelopeXSLTPost() { return this.EnvelopeXSLTPost; }
    public  String getPropEncoding_Out() { return this.PropEncoding_Out; }

    public void setMessageTemplateVO(
            int Template_Id,
            int Interface_Id,
            int Operation_Id,
            int Source_Id,
            String Src_SubCod,
            int Destin_Id,
            String Dst_SubCod,
            String Msg_Type,
            String Msg_Type_own,
            String Template_name,
            String Template_Dir,
            String Log_Level,
            String Conf_Text,
            String LastMaker,
            String LastDate) {

        this.Template_Id = Template_Id ;
        this.Interface_Id = Interface_Id ;
        this.Operation_Id = Operation_Id ;
        this.Source_Id = Source_Id ;
        this.Src_SubCod = Src_SubCod ;
        this.Destin_Id = Destin_Id ;
        this.Dst_SubCod = Dst_SubCod ;
        this.Msg_Type = Msg_Type ;
        this.Msg_Type_own = Msg_Type_own ;
        this.Template_name = Template_name ;
        this.Template_Dir = Template_Dir ;
        this.Log_Level = Log_Level ;
        this.Conf_Text = Conf_Text ;
        this.LastMaker = LastMaker ;
        this.LastDate = LastDate ;
    }
    public int getTemplate_Id(){ return this.Template_Id;  }
    public int getInterface_Id(){ return this.Interface_Id;  }
    public int getOperation_Id(){ return this.Operation_Id ;  }
    public int getSource_Id(){ return this.Source_Id ;  }
    public String getSrc_SubCod(){ return this.Src_SubCod;  }
    public int getDestin_Id(){ return this.Destin_Id;  }
    public String getDst_SubCod(){ return this.Dst_SubCod;  }
    public String getMsg_Type(){ return this.Msg_Type;  }
    public String getMsg_Type_own(){ return this.Msg_Type_own;  }
    public String getTemplate_name(){ return this.Template_name;  }
    public String getTemplate_Dir(){ return this.Template_Dir;  }
    public String getLog_Level(){ return this.Log_Level;  }
    public String getConf_Text(){ return this.Conf_Text;  }
    public String getLastMaker(){ return this.LastMaker;  }
    public String getLastDate(){ return this.LastDate;  }
    public String getHeaderXSLT () {  return this.HeaderXSLT ; }
    public String getMsgAnswXSLT() {  return this.MsgAnswXSLT ; }

    public String getPropExeMetodExecute() { return this.PropExeMetodExecute; }

    public String getPropWebMetod() {  return this.PropWebMetod ; }
    public String getPropExeMetodPostExec() {  return this.PropExeMetodPostExec; }

    public String getPropHostPostExec() {  return this.PropHostPostExec; }
    public String getPropUserPostExec() {  return this.PropUserPostExec;}
    public String getPropPswdPostExec() {  return this.PropPswdPostExec; }
    public String getPropUrlPostExec() {  return this.PropUrlPostExec; }
    public String getPropQueryPostExec() {  return this.PropQueryPostExec; }
    public boolean getPreemptivePostExec() {  return this.isPreemptivePostExec; }

    public String getPropHost() {  return this.PropHost; }
    public String getPropUser() {  return this.PropUser;}
    public String getPropPswd() {  return this.PropPswd; }
    public String getPropUrl() {  return this.PropUrl; }
    public boolean getPreemptive() {  return this.isPreemptive; }
    public String getType_Connection() {  return this. Type_Connection; }


    public int getShortRetryCount(){ return this.ShortRetryCount;}
    public int getShortRetryInterval(){ return this.ShortRetryInterval;}
    public int getLongRetryCount(){ return this.LongRetryCount;}
    public int getLongRetryInterval(){ return this.LongRetryInterval;}

    public Integer getShortRetryCountPostExec(){ return this.ShortRetryCountPostExec;}
    public Integer getShortRetryIntervalPostExec(){ return this.ShortRetryIntervalPostExec;}
    public Integer getLongRetryCountPostExec(){ return this.LongRetryCountPostExec;}
    public Integer getLongRetryIntervalPostExec(){ return this.LongRetryIntervalPostExec;}

    public void setConfigExecute (String ConfigExecute) { this.ConfigExecute = ConfigExecute; }
    public void setConfigPostExec (String ConfigPostExec) { this.ConfigPostExec = ConfigPostExec ; }
    public void setMessageXSD (String MessageXSD) { this.MessageXSD = MessageXSD ; }
    public void setMessageXSLT (String MessageXSLT) { this.MessageXSLT = MessageXSLT ; }
    public void setEnvelopeXSLTExt (String EnvelopeXSLTExt) { this.EnvelopeXSLTExt = EnvelopeXSLTExt ; }
    public void setEnvelopeXSLTPost (String EnvelopeXSLTPost) { this.EnvelopeXSLTPost = EnvelopeXSLTPost ; }
    public void setEnvelopeNS (String EnvelopeNS) { this.EnvelopeNS = EnvelopeNS ; }
    public void setMessageAck (String MessageAck) { this.MessageAck = MessageAck ; }
    public void setMsgAnswXSLT (String MsgAnswXSLT) { this.MsgAnswXSLT = MsgAnswXSLT ; }
    public void setMessageAnswAck (String MessageAnswAck) { this.MessageAnswAck = MessageAnswAck ; }
    public void setMessageAnswerXSD (String MessageAnswerXSD) { this.MessageAnswerXSD = MessageAnswerXSD ; }
    public void setMessageAnswMsgXSLT (String MessageAnswMsgXSLT) { this.MessageAnswMsgXSLT = MessageAnswMsgXSLT ; }

    public void setAckXSD (String AckXSD) { this.AckXSD =  AckXSD; }
    public void setAckXSLT (String AckXSLT) { this.AckXSLT = AckXSLT ; }
    public void setAckAnswXSLT (String AnswAckXSLT) { this.AckAnswXSLT = AnswAckXSLT ; }

    public void setErrTransXSLT (String ErrTransXSLT) { this.ErrTransXSLT =  ErrTransXSLT ; }
    public void setErrTransXSD (String ErrTransXSD) { this.ErrTransXSD = ErrTransXSD ; }
    public void setHeaderXSD (String HeaderXSD ) { this.HeaderXSD  = HeaderXSD  ; }
    public void setHeaderXSLT (String HeaderXSLT) { this.HeaderXSLT = HeaderXSLT ; }


}
