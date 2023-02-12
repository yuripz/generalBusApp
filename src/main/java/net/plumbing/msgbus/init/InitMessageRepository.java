package net.plumbing.msgbus.init;

import net.plumbing.msgbus.common.DataAccess;
import net.plumbing.msgbus.common.sStackTracе;
import org.slf4j.Logger;
import net.plumbing.msgbus.model.MessageDirectionsVO;
import net.plumbing.msgbus.model.MessageDirections;
import net.plumbing.msgbus.model.MessageTypeVO;
import net.plumbing.msgbus.model.MessageType;
import net.plumbing.msgbus.model.MessageTemplateVO;
import net.plumbing.msgbus.model.MessageTemplate;

import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;


import java.sql.*;

public class InitMessageRepository {
    private static PreparedStatement stmtMsgTypeReRead;
    private static PreparedStatement stmtMsgDirectionReRead;
    private static PreparedStatement stmtMsgTemplateReRead;

/* Перечитывать перечень систем бессмысленно, потоки и их конфигурация уже сформированы
    public static  int ReReadMsgDirections( String HrmsSchema, Long intervalReInit, Long CurrentTime,
            int ShortRetryCount, int ShortRetryInterval, int LongRetryCount, int LongRetryInterval,
            Logger AppThead_log )
    {         }
*/
    public static  int ReReadMsgTypes(Long intervalReInit, Logger AppThead_log )  {

        int MessageTypeVOkey;
        ResultSet rs = null;

        if ( DataAccess.Hermes_Connection == null )
        {  AppThead_log.error("ReReadMsgTypes: DataAccess.Hermes_Connection == null");
            return -3;
        }
        PreparedStatement stmtMsgTypeReRead=null;
        String selectMsgTypeReRead;
        if ( DataAccess.rdbmsVendor.equalsIgnoreCase("oracle"))
            //  Oracle date - YYYY-MM-DD HH24:MI:SS
            selectMsgTypeReRead = "select t.interface_id, " +
                    "t.operation_id, t.msg_type, t.msg_type_own, t.msg_typedesc, t.msg_direction, " +
                    "t.msg_handler, t.url_soap_send, t.url_soap_ack, t.max_retry_count, t.max_retry_time " +
                    "from " + DataAccess.HrmsSchema + ".MESSAGE_typeS t " +
                        "where (1=1) and t.msg_direction like '%IN%' " +
                        "and t.LAST_UPDATE_DT > ( sysDate  -  ( 180 + " + intervalReInit + " )/(24*3600)  )" +
                        "order by t.interface_id, t.operation_id";
        else //  PostGree
            selectMsgTypeReRead = "select t.interface_id, " +
                    "t.operation_id, t.msg_type, t.msg_type_own, t.msg_typedesc, t.msg_direction, " +
                    "t.msg_handler, t.url_soap_send, t.url_soap_ack, t.max_retry_count, t.max_retry_time " +
                    "from " + DataAccess.HrmsSchema + ".MESSAGE_typeS t " +
                        "where (1=1) and t.msg_direction like '%IN%' " +
                        "and t.LAST_UPDATE_DT >  ( clock_timestamp()  - Interval '1 Second' * ( 180 + " + intervalReInit + " )  )" +
                        "order by t.interface_id, t.operation_id";
        try {
            stmtMsgTypeReRead = DataAccess.Hermes_Connection.prepareStatement( selectMsgTypeReRead );
            AppThead_log.info( "selectMsgTypeReRead: \n" + selectMsgTypeReRead + " ;" );

            PreparedStatement stmtMsgType = stmtMsgTypeReRead;
            //stmtMsgType.setTimestamp(1, DataAccess.InitDate );
            rs = stmtMsgType.executeQuery();
            while (rs.next()) {

                MessageTypeVOkey  = MessageRepositoryHelper.look4MessageTypeVO_by_Operation_Id(
                        rs.getInt("interface_id"),
                         rs.getInt("operation_id"), AppThead_log );
                if ( MessageTypeVOkey >= 0 ) {
                    AppThead_log.info("Update MessageTypes[" +   MessageTypeVOkey + "]: Msg_Type " + MessageType.AllMessageType.get( MessageTypeVOkey ).getMsg_Type() );
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setURL_SOAP_Send( rs.getString("url_soap_send") );
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setMax_Retry_Count( rs.getInt("max_retry_count") );
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setMax_Retry_Time( rs.getInt("max_retry_time"));
                    AppThead_log.info(" Types ["+ MessageTypeVOkey + "] URL_SOAP_Send=" + MessageType.AllMessageType.get( MessageTypeVOkey ).getURL_SOAP_Send());
                }
                else {
                    MessageTypeVO messageTypeVO = new MessageTypeVO();
                    messageTypeVO.setMessageTypeVO(
                            rs.getInt("interface_id"),
                            rs.getInt("operation_id"),
                            rs.getString("msg_type"),
                            rs.getString("msg_type_own"),
                            rs.getString("msg_typedesc"),
                            rs.getString("msg_direction"),
                            rs.getInt("msg_handler"),
                            rs.getString("url_soap_send"),
                            rs.getString("url_soap_ack"),
                            rs.getInt("max_retry_count"),
                            rs.getInt("max_retry_time")
                    );

                    MessageType.AllMessageType.put(MessageType.RowNum, messageTypeVO);
                    MessageType.RowNum += 1;
                    AppThead_log.info(" Types.size=" + MessageType.AllMessageType.size() + ", MessageRowNum[" + MessageType.RowNum + "] :" + messageTypeVO.getMsg_Type());
                }
            }
            rs.close();
            stmtMsgTypeReRead.close();
            DataAccess.Hermes_Connection.commit();
            //stmtMsgType.close();
        } catch (Exception e) {
            AppThead_log.error("ReReadMsgTypes (" + selectMsgTypeReRead + ") fault: " + sStackTracе.strInterruptedException(e));
            try {
                if ( stmtMsgTypeReRead != null) stmtMsgTypeReRead.close();
                DataAccess.Hermes_Connection.rollback();
            } catch ( SQLException SQLe) {
                AppThead_log.error("rollback (" + selectMsgTypeReRead + ") fault: " + sStackTracе.strInterruptedException(SQLe));
                SQLe.printStackTrace();
            }
            // e.printStackTrace();
            return -2;
        }
        return MessageType.RowNum;
    }

    public static  int ReReadMsgTemplates(Long intervalReInit, Logger AppThead_log )  {
        int parseResult;
        int MessageTemplateVOkey;

        ResultSet rs;
        Logger log = AppThead_log;

        if ( DataAccess.Hermes_Connection == null )
        {  AppThead_log.error("ReReadMsgTypes: DataAccess.Hermes_Connection == null");
            return -3;
        }
        PreparedStatement stmtMsgTemplateReRead;
        String selectMsgTemplateReRead;
        if ( DataAccess.rdbmsVendor.equalsIgnoreCase("oracle"))
            // todo Oracle
            selectMsgTemplateReRead = "select t.template_id, t.interface_id, t.operation_id, t.msg_type, t.msg_type_own, " +
                "t.template_name, t.template_dir, t.source_id, t.destin_id, t.conf_text, t.src_subcod, " +
                "t.dst_subcod, t.lastmaker, t.lastdate " +
                "from " + DataAccess.HrmsSchema + ".MESSAGE_TemplateS t " +
                "where (1=1) " +
                 "and t.LastDate > ( sysDate - (180 + " + intervalReInit + " )/(24*3600))" +
                 // "and t.LastDate > ( to_date('" + DataAccess.dateFormat.format( DataAccess.InitDate ) +"', 'YYYY-MM-DD HH24:MI:SS') - (" + intervalReInit + "+ 180)/(60*60*24)) " +
                "and t.template_dir like '%IN%' " +
                "order by t.interface_id, t.operation_id, t.destin_id, t.dst_subcod";
        else // todo PostGree
            selectMsgTemplateReRead = "select t.template_id, t.interface_id, t.operation_id, t.msg_type, t.msg_type_own, " +
                    "t.template_name, t.template_dir, t.source_id, t.destin_id, t.conf_text, t.src_subcod, " +
                    "t.dst_subcod, t.lastmaker, t.lastdate " +
                    "from " + DataAccess.HrmsSchema + ".MESSAGE_TemplateS t " +
                    "where (1=1) " +
                    "and t.LastDate >  ( clock_timestamp()  - Interval '1 Second' * ( 180 + " + intervalReInit + " )  )" +
                    "and t.template_dir like '%IN%' " +  // "and t.operation_id in (0, 154 ) " +
                    "order by t.interface_id, t.operation_id, t.destin_id, t.dst_subcod";

        try {
            stmtMsgTemplateReRead = DataAccess.Hermes_Connection.prepareStatement( selectMsgTemplateReRead );
            PreparedStatement stmtMsgTemplate = stmtMsgTemplateReRead;
            AppThead_log.info("selectMsgTemplateReRead: \n" + selectMsgTemplateReRead + " ;" );
            // stmtMsgTemplate.setTimestamp(1, DataAccess.InitDate );
            rs = stmtMsgTemplate.executeQuery();
            while (rs.next()) {
                AppThead_log.info("ReReadMsgTemplates: Обновляем template_id[" + rs.getInt("template_id") + "]");
                MessageTemplateVOkey  = MessageRepositoryHelper.look4MessageTemplate(rs.getInt("template_id"), AppThead_log );
                if ( MessageTemplateVOkey >= 0 ) {
                    MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey );
                    messageTemplateVO.setInterface_Id( rs.getInt("Interface_Id") );

                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setInterface_Id( rs.getInt("Interface_Id") );
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setOperation_Id( rs.getInt("Operation_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setSource_Id(rs.getInt("Source_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setSrc_SubCod(rs.getString("Src_SubCod"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setDestin_Id(rs.getInt("Destin_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setDst_SubCod(rs.getString("Dst_SubCod"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setMsg_Type(rs.getString("Msg_Type"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setMsg_Type_own(rs.getString("Msg_Type_own"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setTemplate_name(rs.getString("Template_name"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setTemplate_Dir(rs.getString("Template_Dir"));
//                        rs.getString("Log_Level"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLog_Level("INFO");
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setConf_Text(rs.getString("Conf_Text"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLastMaker(rs.getString("LastMaker"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLastDate(rs.getString("LastDate"));
                    parseResult = ConfigMsgTemplates.performConfig(MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ), log);
                }
                else {

                    MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
                    messageTemplateVO.setMessageTemplateVO(
                            rs.getInt("template_id"),
                            rs.getInt("Interface_Id"),
                            rs.getInt("Operation_Id"),
                            rs.getInt("Source_Id"),
                            rs.getString("Src_SubCod"),
                            rs.getInt("Destin_Id"),
                            rs.getString("Dst_SubCod"),
                            rs.getString("Msg_Type"),
                            rs.getString("Msg_Type_own"),
                            rs.getString("Template_name"),
                            rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
                            "INFO",
                            rs.getString("Conf_Text"),
                            rs.getString("LastMaker"),
                            rs.getString("LastDate")
                    );
                    //messageTypeVO.LogMessageDirections( log );
                    //log.info(" MessageTemplateVO :", MessageTemplateVO. );


                    // log.info(" Directions.size :" +  MessageTemplate.AllMessageTemplate.size() );

                    parseResult = ConfigMsgTemplates.performConfig(messageTemplateVO, log);
                    MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);

                    log.info(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                            " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() + " parseConfigResult=" + parseResult);

                    MessageTemplate.RowNum += 1;
                }

            }
            rs.close();
            stmtMsgTemplateReRead.close();
            DataAccess.Hermes_Connection.commit();
            // stmtMsgTemplate.close();
        } catch (Exception e) {
            AppThead_log.error("ReReadMsgTemplates: (" + selectMsgTemplateReRead + ") fault: " + sStackTracе.strInterruptedException(e));
            try {
                DataAccess.Hermes_Connection.rollback();
            } catch ( SQLException SQLe) {
                AppThead_log.error("ReReadMsgTemplates: rollback() fault: " + sStackTracе.strInterruptedException(SQLe));
                SQLe.printStackTrace();
            }
            return -2;
        }
        return MessageTemplate.RowNum;
    }

    public static  int SelectMsgDirections(
            int ShortRetryCount, int ShortRetryInterval, int LongRetryCount, int LongRetryInterval,
            Logger AppThead_log )  {
        PreparedStatement stmtMsgDirection = null;
        ResultSet rs = null;
        Logger log = AppThead_log;

        MessageDirections.AllMessageDirections.clear();

        if ( DataAccess.Hermes_Connection != null )
            try {
                // stmtMsgDirectionReRead = DataAccess.Hermes_Connection.prepareStatement(SQLMsgDirectionReRead );
                
                stmtMsgDirection = DataAccess.Hermes_Connection.prepareStatement("SELECT f.msgdirection_id," +
                        " f.msgdirection_cod," +
                        " f.msgdirection_desc," +
                        " f.app_server," +
                        " f.wsdl_name," +
                        " f.msgdir_own," +
                        " f.operator_id," +
                        " f.type_connect," +
                        " f.db_name," +
                        " f.db_user," +
                        " f.db_pswd," +
                        " f.subsys_cod," +
                        " f.base_thread_id," +
                        " f.num_thread," +
                        " COALESCE(f.Short_retry_count,0) Short_retry_count, " +
                        " COALESCE(f.Short_retry_interval,0) Short_retry_interval," +
                        " COALESCE(f.Long_retry_count,0) Long_retry_count, " +
                        " COALESCE(f.Long_retry_interval,0) Long_retry_interval" +
                        " FROM " + DataAccess.HrmsSchema + ".Message_Directions f" +
                        " order by f.msgdirection_iD,  f.subsys_cod,  f.msgdirection_cod");
                
            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgDirection.executeQuery();
            while (rs.next()) {
                MessageDirectionsVO messageDirectionsVO = new MessageDirectionsVO();
                messageDirectionsVO.setMessageDirectionsVO(
                        rs.getInt("msgdirection_id"),
                        rs.getString("msgdirection_cod"),
                        rs.getString("Msgdirection_Desc"),
                        rs.getString("app_server"),
                        rs.getString("wsdl_name"),
                        rs.getString("msgdir_own"),
                        rs.getString("operator_id"),
                        rs.getInt("type_connect"),
                        rs.getString("db_name"),
                        rs.getString("db_user"),
                        rs.getString("db_pswd"),
                        rs.getString("subsys_cod"),
                        rs.getInt("base_thread_id"),
                        rs.getInt("num_thread"),
                        rs.getInt("short_retry_count"),
                        rs.getInt("short_retry_interval"),
                        rs.getInt("long_retry_count"),
                        rs.getInt("long_retry_interval")
                );
               // messageDirectionsVO.LogMessageDirections( log );
                if (messageDirectionsVO.getShort_retry_count() == 0 )
                    messageDirectionsVO.setShort_retry_count( ShortRetryCount );

                if (messageDirectionsVO.getShort_retry_interval() == 0 )
                    messageDirectionsVO.setShort_retry_interval( ShortRetryInterval );

                if (messageDirectionsVO.getLong_retry_count() == 0 )
                    messageDirectionsVO.setLong_retry_count( LongRetryCount );

                if (messageDirectionsVO.getLong_retry_interval() == 0 )
                    messageDirectionsVO.setLong_retry_interval( LongRetryInterval );

                MessageDirections.AllMessageDirections.put( MessageDirections.RowNum, messageDirectionsVO );
                log.info( "RowNum[" + MessageDirections.RowNum + "] =" + messageDirectionsVO.LogMessageDirections() );

                MessageDirections.RowNum += 1;

                // log.info(" MessageDirections[" +   MessageDirections.AllMessageDirections.size() + "]: longRetryInterval=" + messageDirectionsVO.getLong_retry_interval() + ", "+ messageDirectionsVO.getMsgDirection_Desc() );

            }
            rs.close();
            //  stmtMsgDirectionReRead.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
           return MessageDirections.RowNum;
    }

    public static  int SelectMsgTypes( Logger AppThead_log )  {
        PreparedStatement stmtMsgType = null;
        ResultSet rs = null;
        Logger log = AppThead_log;

        MessageType.AllMessageType.clear();
        MessageType.RowNum = 0;

        if ( DataAccess.Hermes_Connection != null )
            try {
                stmtMsgType = DataAccess.Hermes_Connection.prepareStatement("select t.interface_id, " +
                        "t.operation_id, " +
                        "t.msg_type, " +
                        "t.msg_type_own, " +
                        "t.msg_typedesc, " +
                        "t.msg_direction, " +
                        "t.msg_handler, " +
                        "t.url_soap_send, " +
                        "t.url_soap_ack, " +
                        "t.max_retry_count, " +
                        "t.max_retry_time " +
                        "from " + DataAccess.HrmsSchema + ".MESSAGE_typeS t " +
                        "where (1=1) " +
                        "and t.msg_direction like '%IN%' " +
                        "order by t.interface_id, t.operation_id");

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgType.executeQuery();
            while (rs.next()) {
                MessageTypeVO messageTypeVO = new MessageTypeVO();
                messageTypeVO.setMessageTypeVO(
                        rs.getInt("interface_id"),
                        rs.getInt("operation_id"),
                        rs.getString("msg_type"),
                        rs.getString("msg_type_own"),
                        rs.getString("msg_typedesc"),
                        rs.getString("msg_direction"),
                        rs.getInt("msg_handler"),
                        rs.getString("url_soap_send"),
                        rs.getString("url_soap_ack"),
                        rs.getInt("max_retry_count"),
                        rs.getInt("max_retry_time")
                );
                //messageTypeVO.LogMessageDirections( log );
                // log.info(" messageTypeVO :", messageTypeVO );
                // log.info(" AllMessageType.size :" +   MessageType.AllMessageType.size() );

                    MessageType.AllMessageType.put( MessageType.RowNum, messageTypeVO );
                    MessageType.RowNum += 1;

                log.info(" Types.size=" +   MessageType.AllMessageType.size() + ", MessageRowNum[" + MessageType.RowNum + "] :" + messageTypeVO.getMsg_Type() );
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageType.RowNum;
    }

    public static  int SelectMsgTemplates( Logger AppThead_log )  {
        int parseResult;
        PreparedStatement stmtMsgTemplate;
        ResultSet rs;
        Logger log = AppThead_log;
        //MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        MessageTemplate.AllMessageTemplate.clear();
        MessageTemplate.RowNum = 0;

        if ( DataAccess.Hermes_Connection != null )
            try {

                stmtMsgTemplate = DataAccess.Hermes_Connection.prepareStatement(
                        "select t.template_id, " +
                                "t.interface_id, " +
                                "t.operation_id, " +
                                "t.msg_type, " +
                                "t.msg_type_own, " +
                                "t.template_name, " +
                                "t.template_dir, " +
                                "t.source_id, " +
                                "t.destin_id, " +
                                "t.conf_text, " +
                                "t.src_subcod, " +
                                "t.dst_subcod, " +
                                "t.lastmaker, " +
                                "to_char(t.lastdate,'YYYY.MM.DD HH24:MI:SS') LastDate " +
                        "from " + DataAccess.HrmsSchema +  ".MESSAGE_TemplateS t " +
                        "where (1=1) and t.template_dir like '%IN%' " +
                                "order by t.interface_id, t.operation_id, t.destin_id, t.dst_subcod");

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgTemplate.executeQuery();
            while (rs.next()) {
                MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
                messageTemplateVO.setMessageTemplateVO(
                        rs.getInt("template_id"),
                        rs.getInt("Interface_Id"),
                        rs.getInt("Operation_Id"),
                        rs.getInt("Source_Id"),
                        rs.getString("Src_SubCod"),
                        rs.getInt("Destin_Id"),
                        rs.getString("Dst_SubCod"),
                        rs.getString("Msg_Type"),
                        rs.getString("Msg_Type_own"),
                        rs.getString("Template_name"),
                        rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
                        "INFO",
                        rs.getString("Conf_Text"),
                        rs.getString("LastMaker"),
                        rs.getString("LastDate")
                );
                //messageTypeVO.LogMessageDirections( log );
                //log.info(" MessageTemplateVO :", MessageTemplateVO. );


                // log.info(" Directions.size :" +  MessageTemplate.AllMessageTemplate.size() );

                parseResult = ConfigMsgTemplates.performConfig(messageTemplateVO, log);
                MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);

                log.info(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                        " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() +" parseConfigResult=" + parseResult);

                MessageTemplate.RowNum += 1;


            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageTemplate.RowNum;
    }


}