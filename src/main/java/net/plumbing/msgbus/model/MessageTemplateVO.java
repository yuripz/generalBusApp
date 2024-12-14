package net.plumbing.msgbus.model;

public class MessageTemplateVO {


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
    private String MessageXSLTNew;
    private String EnvelopeXSLTPost;
    private String EnvelopeNS;
    private String fixMessageNS;
    private String MessageAck;
    private String MsgAnswXSLT;
    private String MessageAnswAck;
    private String MessageAnswerXSD;
    private String MessageAnswMsgXSLT;
    private String MessageAnswHdXSLT;
    private String AckXSD;
    private String AckXSLT;
    private String AckAnswXSLT;
    private String AnswAckHdXSLT;
    private String ErrTransXSLT;
    private String ErrTransXSD;
    private String HeaderXSD;
    private String HeaderXSLT;
    private String HeaderInXSLT;

    private String WsdlInterface;
    private String WsdlXSD1;
    private String WsdlXSD2;
    private String WsdlXSD3;
    private String WsdlXSD4;
    private String WsdlXSD5;
    private String WsdlXSD6;
    private String WsdlXSD7;
    private String WsdlXSD8;
    private String WsdlXSD9;

    public void setEnvelopeInXSLT(String EnvelopeInXSLT) { this.EnvelopeInXSLT = EnvelopeInXSLT; }
    public String getEnvelopeInXSLT() { return this.EnvelopeInXSLT; }

    public void setHeaderInXSLT(String HeaderInXSLT) { this.HeaderInXSLT = HeaderInXSLT; }
    public String getHeaderInXSLT() { return this.HeaderInXSLT; }

    public void setWsdlInterface(String WsdlInterface) { this.WsdlInterface = WsdlInterface; }
    public String getWsdlInterface() { return this.WsdlInterface; }

    public void setWsdlXSD1 (String WsdlXSD1) { this.WsdlXSD1 = WsdlXSD1 ; }
    public String getWsdlXSD1() { return this.WsdlXSD1; }
    public void setWsdlXSD2 (String WsdlXSD2) { this.WsdlXSD2 = WsdlXSD2 ; }
    public String getWsdlXSD2() { return this.WsdlXSD2; }
    public void setWsdlXSD3 (String WsdlXSD3) { this.WsdlXSD3 = WsdlXSD3 ; }
    public String getWsdlXSD3() { return this.WsdlXSD3; }
    public void setWsdlXSD4 (String WsdlXSD4) { this.WsdlXSD4 = WsdlXSD4 ; }
    public String getWsdlXSD4() { return this.WsdlXSD4; }

    public void setWsdlXSD5 (String WsdlXSD5) { this.WsdlXSD5 = WsdlXSD5 ; }
    public String getWsdlXSD5() { return this.WsdlXSD5; }
    public void setWsdlXSD6 (String WsdlXSD6) { this.WsdlXSD6 = WsdlXSD6 ; }
    public String getWsdlXSD6() { return this.WsdlXSD6; }
    public void setWsdlXSD7 (String WsdlXSD7) { this.WsdlXSD7 = WsdlXSD7 ; }
    public String getWsdlXSD7() { return this.WsdlXSD7; }
    public void setWsdlXSD8 (String WsdlXSD8) { this.WsdlXSD8 = WsdlXSD8 ; }
    public String getWsdlXSD8() { return this.WsdlXSD8; }
    public void setWsdlXSD9 (String WsdlXSD9) { this.WsdlXSD9 = WsdlXSD9 ; }
    public String getWsdlXSD9() { return this.WsdlXSD9; }


    private String PropHost;
    private String PropUser;
    private String PropPswd;
    private String PropUrl;
    private String PropEncoding_Out;
    private String PropEncoding_In;
    private String PropTimeout_Conn;
    private String PropTimeout_Read;
    private String PropMaxRetryCnt;
    private String PropMaxRetryTime;



    public static final String  PropNameWebHost  = "host";
    public static final String  PropNameWebUser  = "user";
    public static final String  PropNameWebPswd  = "pswd";
    public static final String  PropNameWebUrl   = "url";
    public static final String  PropNameCharOut  = "encoding_out";
    public static final String  PropNameCharIn   = "encoding_in";
    public static final String  PropCustomFaultBegin   = "custom_Fault_Begin";
    public static final String  PropCustomFaultEnd   = "custom_Fault_End";
    public static final String  PropDebug = "debug";
    public static final String  PropNameConnTimeOut = "timeout_conn";
    public static final String  PropNameReadTimeOut = "timeout_read";
    public static final String  PropNameMaxRetryCnt  = "max_retry_count";
    public static final String  PropNameMaxRetryTime = "max_retry_minut";

    public static final String  PropNameJavaClass     = "class";
    public static final String  PropNameJavaJndiConn  = "jndi_conn";
    public static final String  PropNameShName        = "script";
    public static final String  PropNameParamPref     = "ParamList";

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


    public void setTemplate_Id(int Template_Id){ this.Template_Id = Template_Id;  }
    public void setInterface_Id(int Interface_Id){  this.Interface_Id =Interface_Id;  }
    public void setOperation_Id(int Operation_Id){ this.Operation_Id = Operation_Id ;  }
    public void setSource_Id(int Source_Id){  this.Source_Id = Source_Id ;  }
    public void setSrc_SubCod(String Src_SubCod){  this.Src_SubCod = Src_SubCod;  }
    public void setDestin_Id(int Destin_Id){  this.Destin_Id = Destin_Id;  }
    public void setDst_SubCod(String Dst_SubCod){  this.Dst_SubCod = Dst_SubCod;  }
    public void setMsg_Type(String Msg_Type){  this.Msg_Type = Msg_Type;  }
    public void setMsg_Type_own(String Msg_Type_own){  this.Msg_Type_own = Msg_Type_own;  }
    public void setTemplate_name(String Template_name){  this.Template_name = Template_name;  }
    public void setTemplate_Dir(String Template_Dir){  this.Template_Dir = Template_Dir;  }
    public void setLog_Level(String Log_Level){  this.Log_Level = Log_Level;  }
    public void setConf_Text(String Conf_Text){  this.Conf_Text = Conf_Text;  }
    public void setLastMaker(String LastMaker){  this.LastMaker = LastMaker;  }
    public void setLastDate(String LastDate){  this.LastDate = LastDate;  }


    public void setConfigExecute (String ConfigExecute) { this.ConfigExecute = ConfigExecute; }
    public String getConfigExecute() { return this.ConfigExecute; }


    public void setConfigPostExec (String ConfigPostExec) { this.ConfigPostExec = ConfigPostExec ; }
    public String getConfigPostExec() { return this.ConfigPostExec; }

    public void setMessageXSD (String MessageXSD) { this.MessageXSD = MessageXSD ; }
    public String getMessageXSD() { return this.MessageXSD; }

    public void setMessageXSLT (String MessageXSLT) { this.MessageXSLT = MessageXSLT ; }
    public String getMessageXSLT () { return this.MessageXSLT; }

    public void setEnvelopeXSLTExt (String EnvelopeXSLTExt) { this.EnvelopeXSLTExt = EnvelopeXSLTExt ; }
    public String getEnvelopeXSLTExt () { return this.EnvelopeXSLTExt; }

    public void setEnvelopeXSLTPost (String EnvelopeXSLTPost) { this.EnvelopeXSLTPost = EnvelopeXSLTPost ; }
    public String getEnvelopeXSLTPost () { return this.EnvelopeXSLTPost; }

    public void setEnvelopeNS (String EnvelopeNS) { this.EnvelopeNS = EnvelopeNS ; }
    public String getEnvelopeNS () { return this.EnvelopeNS; }

    public void setMessageAck (String MessageAck) { this.MessageAck = MessageAck ; }
    public String getMessageAck () { return this.MessageAck; }

    public void setMsgAnswXSLT (String MsgAnswXSLT) { this.MsgAnswXSLT = MsgAnswXSLT ; }
    public String getMsgAnswXSLT () { return this.MsgAnswXSLT; }

    public void setMessageAnswAck (String MessageAnswAck) { this.MessageAnswAck = MessageAnswAck ; }
    public String getMessageAnswAck () { return this.MessageAnswAck; }

    public void setMessageAnswerXSD (String MessageAnswerXSD) { this.MessageAnswerXSD = MessageAnswerXSD ; }
    public String getMessageAnswerXSD () { return this.MessageAnswerXSD; }

    public void setMessageAnswMsgXSLT (String MessageAnswMsgXSLT) { this.MessageAnswMsgXSLT = MessageAnswMsgXSLT ; }
    public String getMessageAnswMsgXSLT () { return this.MessageAnswMsgXSLT; }

    public void setMessageAnswHdXSLT (String MessageAnswHdXSLT) { this.MessageAnswHdXSLT = MessageAnswHdXSLT ; }
    public String getMessageAnswHdXSLT () { return this.MessageAnswHdXSLT; }

    public void setAckXSD (String AckXSD) { this.AckXSD =  AckXSD; }
    public String getAckXSD () { return this.AckXSD; }

    public void setAckXSLT (String AckXSLT) { this.AckXSLT = AckXSLT ; }
    public String getAckXSLT () { return this.AckXSLT; }

    public void setAckAnswXSLT (String AckAnswXSLT) { this.AckAnswXSLT = AckAnswXSLT ; }
    public String getAckAnswXSLT () { return this.AckAnswXSLT; }

    public void setAnswAckHdXSLT (String AnswAckHdXSLT) { this.AnswAckHdXSLT = AnswAckHdXSLT ; }
    public String getAnswAckHdXSLT () { return this.AnswAckHdXSLT; }

    public void setErrTransXSLT (String ErrTransXSLT) { this.ErrTransXSLT =  ErrTransXSLT ; }
    public String getErrTransXSLT () { return this.ErrTransXSLT; }

    public void setErrTransXSD (String ErrTransXSD) { this.ErrTransXSD = ErrTransXSD ; }
    public String getErrTransXSD() { return this.ErrTransXSD; }

    public void setHeaderXSD (String HeaderXSD) { this.HeaderXSD = HeaderXSD ; }
    public String getHeaderXSD() { return this.HeaderXSD; }

    public void setHeaderXSLT (String HeaderXSLT) { this.HeaderXSLT = HeaderXSLT ; }
    public String getHeaderXSLT () { return this.HeaderXSLT; }

}
