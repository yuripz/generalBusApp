package net.plumbing.msgbus.model;
import org.jdom2.Document;
import org.jdom2.Element;

import java.util.ArrayList;
import java.util.HashMap;


public class MessageDetails {

    public  int MessageRowNum =0;
    public  HashMap<Integer, MessageDetailVO > Message = new HashMap<Integer, MessageDetailVO >();
    public  HashMap<Integer, ArrayList> MessageIndex_by_Tag_Par_Num = new HashMap<Integer, ArrayList>(); // для получения messageDetails.Message.get( messageDetails.RecordIndex_by_TagId.get(Tag_Num) )
    public  int ConfirmationRowNum=0;
    public  HashMap<Integer, MessageDetailVO > Confirmation = new HashMap<Integer, MessageDetailVO >();
    // public StringBuilder XML_MsgOUT = new StringBuilder();
    // public String XML_MsgSEND;
    public String XML_MsgInput;
    public Document Input_Clear_XMLDocument=null;
    public Element Input_Header_Context=null;
    public Element Request_Method=null;
    public StringBuilder XML_Request_Method = new StringBuilder(); // XML, формируется в процессе очистки SOAP от ns: для проследующей проверки по XSD и XSLT преобразованию
    public StringBuilder XML_MsgClear = new StringBuilder();
    // public StringBuilder XML_Envelope4XSLTExt = new StringBuilder(); // XML, формируется в процессе преобразованиия очищенного от ns: SOAP (XML_MsgClear) для проследующего осполнения SQLRequest из EnvelopeXSLTExt
    public StringBuilder Soap_HeaderRequest = new StringBuilder();
    public StringBuilder XML_MsgResponse= new StringBuilder(); // ответ от нашего сервиса в виде XML-STRING
    // public StringBuilder XML_ClearBodyResponse= new StringBuilder(); // очищенный от ns: содержимое Body Response Soap
    // public StringBuilder XML_MsgRESOUT= new StringBuilder(); // результат преобоазования MsgAnswXSLT ( или чистый Response)
    public StringBuilder XML_MsgConfirmation= new StringBuilder(); // <Confirmation>****</Confirmation> результат работы прикладного обработчика зачитан из БД,
                                                                   // который должен быть преобоазован AckXSLT
                                                                    //
    public MessageTemplate4Perform MessageTemplate4Perform;
    // TODO public RowId ROWID_QUEUElog=null;
    public String ROWID_QUEUElog=null;
    public long Queue_Id=-1L;
    public Integer X_Total_Count =0;
    public StringBuilder MsgReason = new StringBuilder();

    public int Message_Tag_Num = 0; // счетчик XML элнментов в Message
    public int Confirmation_Tag_Num = 0; // счетчик XML элнментов в Confirmation
    //public CloseableHttpClient SimpleHttpClient;
    //public CloseableHttpClient RestHermesAPIHttpClient;
    //public SSLContext sslContext;
    //public HttpClientBuilder httpClientBuilder;


    public  MessageDetails() {
        this.Message.clear();
        this.Confirmation.clear();
        this.XML_MsgClear.setLength(0); this.XML_MsgClear.trimToSize();
        this.Soap_HeaderRequest.setLength(0); this.Soap_HeaderRequest.trimToSize();
        this.XML_Request_Method.setLength(0); this.XML_Request_Method.trimToSize();
        this.XML_MsgResponse.setLength(0); this.XML_MsgResponse.trimToSize();
       // this.XML_MsgRESOUT.setLength(0);
        this.MsgReason.setLength(0); this.MsgReason.trimToSize();
        this.XML_MsgConfirmation.setLength(0); this.XML_MsgConfirmation.trimToSize();
       // this.XML_Envelope4XSLTExt.setLength(0);
    }
   //  public void SetHttpClient( CloseableHttpClient simpleHttpClient ) { this.SimpleHttpClient= simpleHttpClient; }
   public void ReInitMessageDetails( ) {
       this.Message.clear();
       this.Confirmation.clear();
       this.XML_MsgClear.setLength(0);
       //this.XML_MsgOUT.setLength(0);
       // this.XML_ClearBodyResponse.setLength(0);
       this.XML_MsgResponse.setLength(0);
       // this.XML_MsgRESOUT.setLength(0);
       this.MsgReason.setLength(0);
       this.XML_MsgConfirmation.setLength(0);
       //this.sslContext = sslContext;
       //this.httpClientBuilder = httpClientBuilder;
       //this.SimpleHttpClient= simpleHttpClient; //  парметры соединения есть только в щаблоне
       //this.RestHermesAPIHttpClient= RestHermesAPIHttpClient;
   }
/*
    public void ReInitMessageDetails( SSLContext sslContext, HttpClientBuilder  httpClientBuilder , CloseableHttpClient simpleHttpClient, CloseableHttpClient RestHermesAPIHttpClient  ) {
        this.Message.clear();
        this.Confirmation.clear();
        this.XML_MsgClear.setLength(0);
        //this.XML_MsgOUT.setLength(0);
        // this.XML_ClearBodyResponse.setLength(0);
        this.XML_MsgResponse.setLength(0);
        // this.XML_MsgRESOUT.setLength(0);
        this.MsgReason.setLength(0);
        this.XML_MsgConfirmation.setLength(0);
        this.sslContext = sslContext;
        this.httpClientBuilder = httpClientBuilder;
        //this.SimpleHttpClient= simpleHttpClient; //  парметры соединения есть только в щаблоне
        this.RestHermesAPIHttpClient= RestHermesAPIHttpClient;
    }
 */
}
