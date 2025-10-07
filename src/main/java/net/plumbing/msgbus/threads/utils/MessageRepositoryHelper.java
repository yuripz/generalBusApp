package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.model.*;
import org.slf4j.Logger;

public class MessageRepositoryHelper {

    public static String look4MessageDirectionsCode_4_Num_Thread( Integer Num_Thread, Logger messageSend_log) {
        int MsgDirection_maxBase_Thread_Id = -1;
        int MsgDirectionVO_4_Direction_Key = -1;
        String MessageDirectionsCode = null;
        for (int j = 0; j < MessageDirections.AllMessageDirections.size(); j++) {
            MessageDirectionsVO messageDirectionsVO = MessageDirections.AllMessageDirections.get(j);
            if (( messageDirectionsVO.getBase_Thread_Id() <= Num_Thread ) &&
                ( messageDirectionsVO.getBase_Thread_Id() + messageDirectionsVO.getNum_Thread() >= Num_Thread ))
            {
                if (messageDirectionsVO.getBase_Thread_Id() > MsgDirection_maxBase_Thread_Id )
                {
                    MsgDirection_maxBase_Thread_Id = messageDirectionsVO.getBase_Thread_Id();
                    MsgDirectionVO_4_Direction_Key = j;
                }
            }
        }
        if ( MsgDirectionVO_4_Direction_Key >= 0 )
            MessageDirectionsCode = MessageDirections.AllMessageDirections.get(MsgDirectionVO_4_Direction_Key).getMsgDirection_Cod();
        return MessageDirectionsCode;
    }


    public static  int look4MessageDirectionsVO_2_MsgDirection_Cod( String MsgDirection_Cod, Logger messageSend_log) {
        int MsgDirectionVO_Key=-1;
        int MsgDirectionVO_4_Direction_Key=-1;
        // messageSend_log.warn("look4MessageDirectionsVO_2_MsgDirection_Cod(`"+ MsgDirection_Cod +  "`): MessageDirections.AllMessageDirections.size()= " + MessageDirections.AllMessageDirections.size() );
        for (int j = 0; j < MessageDirections.AllMessageDirections.size(); j++) {
            MessageDirectionsVO messageDirectionsVO = MessageDirections.AllMessageDirections.get(j);
            // messageSend_log.warn("look4MessageDirectionsVO_2_MsgDirection_Cod: messageDirectionsVO.getMsgDirection_Cod()=`" + messageDirectionsVO.getMsgDirection_Cod() + "`" );
            if ( messageDirectionsVO.getMsgDirection_Cod().equalsIgnoreCase( MsgDirection_Cod ))
            { // messageSend_log.warn("equalsIgnoreCase: messageDirectionsVO.getMsgDirection_Cod()=`" + messageDirectionsVO.getMsgDirection_Cod() + "` == `"+ MsgDirection_Cod + "`" );
                MsgDirectionVO_4_Direction_Key = j;
            }
            // else messageSend_log.warn("equalsIgnoreCase: messageDirectionsVO.getMsgDirection_Cod()=`" + messageDirectionsVO.getMsgDirection_Cod() + "` != `"+ MsgDirection_Cod + "`" );
        }
        if (MsgDirectionVO_4_Direction_Key >= 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_Key;
        return  MsgDirectionVO_Key;
    }

    public static  int look4MessageDirectionsVO_2_Perform(int MessageMsgDirection_id, String MessageSubSys_cod, Logger messageSend_log) {
        int MsgDirectionVO_Key=-1;
        int MsgDirectionVO_4_Direction_Key=-1;
        int MsgDirectionVO_4_Direction_SubSys_Id=-1;

        for (int j = 0; j < MessageDirections.AllMessageDirections.size(); j++) {
            MessageDirectionsVO messageDirectionsVO = MessageDirections.AllMessageDirections.get(j);
            if ( messageDirectionsVO.getMsgDirection_Id() == MessageMsgDirection_id )
            { String DirectionsSubSys_Cod = messageDirectionsVO.getSubsys_Cod();
                if (DirectionsSubSys_Cod == null) // дополнительное или ==0 неправильное, если система имеее суб-код 0, то проблемы     || (DirectionsSubSys_Cod).equals("0")
                //    if ( (DirectionsSubSys_Cod == null) || (DirectionsSubSys_Cod).equals("0") )
                {
                    //  заполнен код ПодСистемы : MESSAGE_DIRECTIONS.subsys_cod == '0' OR MESSAGE_DIRECTIONS.subsys_cod is NULL )
                    MsgDirectionVO_4_Direction_Key = j;
                }
                else {
                    if ( DirectionsSubSys_Cod.equals( MessageSubSys_cod ))
                        MsgDirectionVO_4_Direction_SubSys_Id = j;
                }

            }
        }
        if (MsgDirectionVO_4_Direction_Key >= 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_Key;
        if (MsgDirectionVO_4_Direction_SubSys_Id >= 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_SubSys_Id;
        return  MsgDirectionVO_Key;
    }

    public static  int look4MessageTypeVO_by_Operation_Id(int BusOperationInterfaceId, int BusOperation_Id,   Logger messageSend_log) {
        // messageSend_log.info("look4MessageTypeVO_by_MesssageType [0-" + MessageType.AllMessageType.size() + "]: BusOperationInterfaceId=" +BusOperationInterfaceId + " for " + BusOperationMesssageType);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            int Operation_Id = messageTypeVO.getOperation_Id();
            int InterfaceId = messageTypeVO.getInterface_Id();
                if ( ( InterfaceId == BusOperationInterfaceId) &&
                        ( Operation_Id == BusOperation_Id )
                )
                 //  нашли операцию,
                    return  i;
        }
        return -1;
    }

    public static  int look4MessageTypeVO_2_Perform(int Operation_Id,  Logger messageSend_log) {
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if ( messageTypeVO.getOperation_Id() == Operation_Id ) {    //  нашли операцию,
                return  i;
            }
        }
        return -1;
    }

    public static  String look4MessageURL_SOAP_Send_by_Interface(int pInteface_Id,  Logger messageSend_log) {
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                int Inteface_Id = messageTypeVO.getInterface_Id();
                    if ( Inteface_Id ==  pInteface_Id) {    //  нашли интерфейс ,
                        return messageTypeVO.getURL_SOAP_Send(); // i;
                    }
            }
        }
        return null;
    }

    public static  int look4MessageTypeVO_2_Interface(String pUrl_Soap_Send,  Logger messageSend_log) {
       //  messageSend_log.info("look4MessageTypeVO_2_Interface[0-" + MessageType.AllMessageType.size() + "]:`" + pUrl_Soap_Send+"`");
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            // messageSend_log.info("look4MessageTypeVO[" + i + "]:" + messageTypeVO.getURL_SOAP_Send() + " , " +messageTypeVO.getMsg_TypeDesc());
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                String URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
             //    messageSend_log.info("look4MessageTypeVO[" + i + "]:" + URL_SOAP_Send + " , " +messageTypeVO.getMsg_TypeDesc());
                if ( URL_SOAP_Send != null ) {
                    if ( URL_SOAP_Send.equals(pUrl_Soap_Send) ) {    //  нашли операцию,
                        messageSend_log.info("look4MessageTypeVO ok[{}]: for `{}` == `{}` , Msg_TypeDesc== `{}`", messageTypeVO.getInterface_Id(), pUrl_Soap_Send, URL_SOAP_Send , messageTypeVO.getMsg_TypeDesc());
                        return messageTypeVO.getInterface_Id(); // i;
                    }
                }
            }
        }
        return -1;
    }

    public static  String look4MessageTypeVO_by_MesssageType(final String BusOperationMesssageType,  int BusOperationInterfaceId, Logger messageSend_log) {
         // messageSend_log.info("look4MessageTypeVO_by_MesssageType [0-" + MessageType.AllMessageType.size() + "]: BusOperationInterfaceId=" +BusOperationInterfaceId + " for " + BusOperationMesssageType);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
                String OperationMessageType = messageTypeVO.getMsg_Type();
                int InterfaceId = messageTypeVO.getInterface_Id();
                if ( OperationMessageType != null ) {
                   //  messageSend_log.info("{} look4MessageTypeVO_by_MesssageType MessageOperationId= {} check", OperationMesssageType, messageTypeVO.getOperation_Id() );
                    if (  ( InterfaceId == BusOperationInterfaceId) && // ищем глобально по типу, игнорируя BusOperationInterfaceId
                         ( OperationMessageType.toUpperCase().equals(BusOperationMesssageType.toUpperCase()) )
                       )
                    {    //  нашли операцию,
                        int MessageOperationId = messageTypeVO.getOperation_Id(); // i;
                        //  messageSend_log.info("{} look4MessageTypeVO_by_MesssageType MessageOperationId= {} found", OperationMesssageType, messageTypeVO.getOperation_Id() );
                        return Integer.toString(MessageOperationId);
                    }
                }
        }
        return null;
    }


    public static  Integer look4MessageTypeVO_by_MesssageTypeGlobally(final String BusOperationMesssageType,   Logger messageSend_log) {
        // messageSend_log.info("look4MessageTypeVO_by_MesssageType [0-" + MessageType.AllMessageType.size() + "]: BusOperationInterfaceId=" +BusOperationInterfaceId + " for " + BusOperationMesssageType);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            String OperationMesssageType = messageTypeVO.getMsg_Type();

            if ( OperationMesssageType != null ) {
                if ( // ( InterfaceId == BusOperationInterfaceId) && // ищем глобально по типу, игнорируя BusOperationInterfaceId
                        ( OperationMesssageType.toUpperCase().equals(BusOperationMesssageType.toUpperCase()) )
                )
                {    //  нашли операцию,
                    int MessageOperationId = messageTypeVO.getOperation_Id(); // i;
                    // messageSend_log.info("look4MessageTypeVO_by_MesssageType MessageOperationId=" + MessageOperationId.toString() + " found" );
                    Integer InterfaceId = messageTypeVO.getInterface_Id();
                    return InterfaceId;
                }
            }
        }
        return null;
    }

    public static boolean isNoWaitSender4MessageTypeURL_SOAP_Ack_2_Operation(Integer pOperation_Id,  Logger messageSend_log) {
        // messageSend_log.info("isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation[" + MessageType.AllMessageType.size() + "]:" + pOperation_Id);
        MessageTypeVO messageTypeVO;
        //int Interface_Id=0;
        for (int j = 0; j < MessageType.AllMessageType.size(); j++)
        { // находим тип для текущей операции pOperation_Id
            messageTypeVO = MessageType.AllMessageType.get(j);
            if (messageTypeVO.getOperation_Id() == pOperation_Id ) // нашли обрабатывамую операцию
            {
                String isNoWait4Sender = messageTypeVO.getURL_SOAP_Ack();
                if ( isNoWait4Sender != null ) {
                    messageSend_log.info("NoWait4Sender on MessageTypeURL_SOAP_Ack_2_Operation: found [" + isNoWait4Sender + "] for " + pOperation_Id);
                    return isNoWait4Sender.equalsIgnoreCase("NoWait4Sender");
                }
                else {
                    messageSend_log.info("NoWait4Sender on MessageTypeURL_SOAP_Ack_2_Operation: found as NULL for Operation_Id=" + pOperation_Id);
                    return false;
                }
            }
        }
        messageSend_log.warn( "в MessageType.AllMessageType не нашли pOperation_Id=" +pOperation_Id );
        // не нашли
        return false;
    }
    public static  boolean isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation(Integer pOperation_Id,  Logger messageSend_log) {
        // messageSend_log.info("isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation[" + MessageType.AllMessageType.size() + "]:" + pOperation_Id);
        MessageTypeVO messageTypeVO;
        //int Interface_Id=0;
        for (int j = 0; j < MessageType.AllMessageType.size(); j++)
        { // находим тип для текущей операции  pOperation_Id
            messageTypeVO = MessageType.AllMessageType.get(j);
            if (messageTypeVO.getOperation_Id() == pOperation_Id ) // нашли обрабатывамую операцию
            {
                String isNoConfirmation = messageTypeVO.getURL_SOAP_Ack();
                if ( isNoConfirmation != null ) {
                  //  messageSend_log.info("isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation: found [" + isNoConfirmation + "] for " + pOperation_Id);
                    return isNoConfirmation.equalsIgnoreCase("NoConfirmation");
                }
                else {
                   // messageSend_log.info("isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation: found as NULL for Operation_Id=" + pOperation_Id);
                    return false;
                }
            }
        }
        messageSend_log.warn( "в MessageType.AllMessageType не нашли pOperation_Id=" +pOperation_Id );
        // не нашли
        return false;
    }

    public static  boolean isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(String Url_Soap_Send,  Logger messageSend_log) {
        //messageSend_log.info("isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                String URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
                if ( URL_SOAP_Send != null ) {
                    if ( URL_SOAP_Send.equals(Url_Soap_Send) ) {    //  нашли операцию,
                        String isRest = messageTypeVO.getURL_SOAP_Ack();
                        if ( isRest != null )
                            return isRest.toUpperCase().contains("REST"); // в формируемое для преобразования не будет добвленр <Body></Body>
                        // return isRest.equalsIgnoreCase("REST"); //
                        else
                            return false;
                    }
                }
            }
        }
        messageSend_log.warn("isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send + " не нашёл итерфейса по URL");
        return false;
    }

    public static  boolean isLooked4MessageTypeURL_SOAP_Ack_RestExel_2_Interface(String Url_Soap_Send,  Logger messageSend_log) {
        //messageSend_log.info("isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                String URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
                if ( URL_SOAP_Send != null ) {
                    if ( URL_SOAP_Send.equals(Url_Soap_Send) ) {    //  нашли операцию,
                        String isRest = messageTypeVO.getURL_SOAP_Ack();
                        if ( isRest != null )
                            return isRest.equalsIgnoreCase("REST-EXCEL")
                                    ; // в формируемое для преобразования не будет добвленр <Body></Body>
                        else
                            return false;
                    }
                }
            }
        }
        messageSend_log.warn("isLooked4MessageTypeURL_SOAP_Ack_RestExel_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send + " не нашёл итерфейса по URL");
        return false;
    }

    public static  boolean isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface(String Url_Soap_Send,  Logger messageSend_log) {
        //messageSend_log.info("isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                String URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
                if ( URL_SOAP_Send != null ) {
                    if ( URL_SOAP_Send.equals(Url_Soap_Send) ) {    //  нашли операцию,
                        String isRest = messageTypeVO.getURL_SOAP_Ack();
                        if ( isRest != null )
                                 return isRest.equalsIgnoreCase("REST-XML") ||
                                        isRest.equalsIgnoreCase("REST-EXCEL")
                                         ; // в формируемое для преобразования не будет добвленр <Body></Body>
                        else
                            return false;
                    }
                }
            }
        }
        messageSend_log.warn("isLooked4MessageTypeURL_SOAP_Ack_RestXML_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send + " не нашёл итерфейса по URL");
        return false;
    }
/*  не используется, вместо неё isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation
    public static  boolean isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface(Integer pOperation_Id,  Logger messageSend_log) {
        messageSend_log.info("isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + pOperation_Id);

        MessageTypeVO messageTypeVO;
        int Interface_Id=0;
        for (int j = 0; j < MessageType.AllMessageType.size(); j++)
        { // находим тип для текущей операции  pOperation_Id
             messageTypeVO = MessageType.AllMessageType.get(j);
            if (messageTypeVO.getOperation_Id() == pOperation_Id ) // нашли обрабатывамую операцию
            {
                Interface_Id = messageTypeVO.getInterface_Id(); // берём её интерфейс

                for ( int i = 0; i < MessageType.AllMessageType.size(); i++) {
                    messageTypeVO = MessageType.AllMessageType.get(i);
                    if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                        int check_Interface_Id = messageTypeVO.getInterface_Id();
                        if ( check_Interface_Id == Interface_Id ) {  //  нашли интерфейс,
                            String isRest = messageTypeVO.getURL_SOAP_Ack();
                            if ( isRest != null ) {
                                messageSend_log.info("isLooked4MessageTypeURL_SOAP_Ack_Rest_2_Interface: found " + check_Interface_Id + " for " + pOperation_Id);
                                return isRest.equalsIgnoreCase("REST");
                            }
                            else
                                return false;
                        }
                    }
                }
            }
        }
        messageSend_log.warn( "не нашли " );
        // не нашли
        return false;
    }
*/
    public static  int look4MessageTemplate_2_Interface(int look4_Interface_Id,  Logger messageSend_log) {
        messageSend_log.info("look4MessageTemplate_2_Interface[" + MessageTemplate.AllMessageTemplate.size() + "]:" + look4_Interface_Id);
        int MessageTemplateVOkey=-1;

        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            int Operation_Id = messageTemplateVO.getOperation_Id();
            int Interface_Id = messageTemplateVO.getInterface_Id();
             //messageSend_log.info("look4MessageTemplate, проверяем MessageTemplateVOkey=[" + i +"]: Operation_Id =" + Operation_Id + ", Interface_Id =" + Interface_Id );

            if ((Interface_Id == look4_Interface_Id) && ( Operation_Id== 0)) {
                // №№ Шаблонов совпали,  Template_Id = i;
                MessageTemplateVOkey = i;
                messageSend_log.info( "look4MessageTemplate_2_Interface: используем [" + MessageTemplateVOkey +"]: Template_Id=" +
                        MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_Id() +
                        ", Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_name() );
                return MessageTemplateVOkey;
            }
        }
        messageSend_log.info("look4MessageTemplate, получаем MessageTemplateVOkey=[" + MessageTemplateVOkey +"]: значит, не нашли");

        return MessageTemplateVOkey;
    }

    public static int look4MessageTemplate( int look4Template_Id,
                                            Logger messageSend_log) {
        int MessageTemplateVOkey=-1;

        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            int Template_Id = messageTemplateVO.getTemplate_Id();

            if (Template_Id == look4Template_Id) {
                // №№ Шаблонов совпали,  Template_Id = i;
                MessageTemplateVOkey = i;
                messageSend_log.info( "look4MessageTemplate: используем [" + MessageTemplateVOkey +"]: Template_Id=" +
                        MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_Id() +
                        ", Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_name()
                );
                return MessageTemplateVOkey;
            }
        }
        messageSend_log.info("look4MessageTemplate, получаем MessageTemplateVOkey=[" + MessageTemplateVOkey +"]: значит, не нашли");

        return MessageTemplateVOkey;
    }

    public static int look4MessageTemplateVO_2_Perform( int Operation_Id,
                                                  int MsgDirection_Id,
                                                  String  SubSys_Cod  , Logger messageSend_log) {
        int Template_Id=-1;
        int Template_All_Id=-1;
        int Template_4_Direction_Id=-1;
        int Template_4_Direction_SubSys_Id=-1;

        int Type_Id = -1;
        int TemplateOperation_Id ;
        int TemplateMsgDirection_Id;
        String  TemplateSubSys_Cod;
        // 1) пробегаем по Типам сообщений
        for (int i = 0; i < MessageType.AllMessageType.size(); i++)
        {
            MessageTypeVO  messageTypeVO = MessageType.AllMessageType.get(i);
            if ( messageTypeVO.getOperation_Id() == Operation_Id )
            {
                //  нашли операцию,
                Type_Id = i;
            }
        }

        if ( Type_Id < 0) {
            messageSend_log.info("Operation[" + Operation_Id + "] is not found in any MessageType");
            return Template_Id;
        }
        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            TemplateOperation_Id = messageTemplateVO.getOperation_Id();
            TemplateMsgDirection_Id = messageTemplateVO.getSource_Id();
            TemplateSubSys_Cod = messageTemplateVO.getSrc_SubCod();

            // messageSend_log.info("[" + i + "] № операции (" + TemplateOperation_Id + ") TemplateMsgDirection_Id =[" + TemplateMsgDirection_Id + "], TemplateSubSys_Cod=" + TemplateSubSys_Cod );

            if (TemplateOperation_Id == Operation_Id) {
                // № операции совпали,  Template_Id = i;
                //  messageSend_log.info("[" + i + "] № операции (" + Operation_Id + ") совпали =[" + TemplateOperation_Id + "], " + messageTemplateVO.getTemplate_name() );
                //  messageSend_log.info("[" + i + "] Template_Id (" + messageTemplateVO.getTemplate_Id() + ") смотрим TemplateSubSys_Cod =[" + TemplateSubSys_Cod + "]" );

                if ( (TemplateSubSys_Cod == null) || (TemplateSubSys_Cod).equals("0") || (TemplateSubSys_Cod).isEmpty())
                { // в Шаблоне не заполнен код ПодСистемы : MESSAGE_templateS.dst_subcod == '0' OR MESSAGE_templateS.dst_subcod is NULL )
                    // сравниваем по коду сисмемы Шаблона MESSAGE_templateS.destin_id и сообщения MESSAGE_QUEUE.MsgDirection_Id
                    //     messageSend_log.info("сравниваем по коду сисмемы Шаблона MESSAGE_templateS.destin_id " + TemplateMsgDirection_Id + " и сообщения MESSAGE_QUEUE.MsgDirection_Id");

                    if (( TemplateMsgDirection_Id != 0 ) && (TemplateMsgDirection_Id == MsgDirection_Id )){
                        // совпали Идентификаторы систем
                        Template_4_Direction_Id= i;
                        //       messageSend_log.info("Идентификаторы систем (" + MsgDirection_Id + ") совпали[" + TemplateMsgDirection_Id + "]=" + messageTemplateVO.getTemplate_name() );
                    }
                    if ( ( TemplateMsgDirection_Id == 0 )) {
                        // Шаблон для любой системы
                        Template_All_Id = i;
                        //     messageSend_log.info("Шаблон для любой системы(" + messageTemplateVO.getDestin_Id() + ") совпали[" + messageTemplateVO.getTemplate_Id() + "]=" + messageTemplateVO.getTemplate_name() );
                    }

                }
                else { // в Шаблоне Заполнен код ПодСистемы : MESSAGE_templateS.dst_subcod is NOT null -> MESSAGE_templateS.destin_id is NOT null too !
                    // проверяем на полное совпадение
                    //    messageSend_log.info("сравниваем по коду ПОДсистемы Шаблона "+ TemplateSubSys_Cod + " и MESSAGE_templateS.destin_id " + TemplateMsgDirection_Id +
                    //            " с сообщением MESSAGE_QUEUE.SubSys_Cod(" + SubSys_Cod + ") MESSAGE_QUEUE.MsgDirection_Id(" + MsgDirection_Id + ")");
                    if ( (TemplateSubSys_Cod.equals(SubSys_Cod) ) && (TemplateMsgDirection_Id == MsgDirection_Id ) ) {
                        Template_4_Direction_SubSys_Id = i;
                        //     messageSend_log.info("Идентификаторы систем (" + MsgDirection_Id + ") совпали[" + messageTemplateVO.getTemplate_Id() + "]=" + " коды подСистем тоже совпали (" + SubSys_Cod + ") " + messageTemplateVO.getTemplate_name());
                    }
                }

            }
        }
        // уточняем точность находки в порядке широты применения
        if ( Template_All_Id >= 0 ) Template_Id = Template_All_Id;
        if ( Template_4_Direction_Id >= 0 ) Template_Id = Template_4_Direction_Id;
        if ( Template_4_Direction_SubSys_Id >= 0 ) Template_Id = Template_4_Direction_SubSys_Id;
        if ( Template_Id >= 0 )
            messageSend_log.info("Итого, используем [" + Template_Id +"]: Template_Id=" + MessageTemplate.AllMessageTemplate.get(Template_Id).getTemplate_Id());
        else
            messageSend_log.error("Итого, получаем Template_Id=[" + Template_Id +"]: значит, не нашли");

        return Template_Id;

    }
}
