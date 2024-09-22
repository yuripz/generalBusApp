package net.plumbing.msgbus.threads;

import com.zaxxer.hikari.HikariDataSource;
// import oracle.jdbc.internal.PreparedStatement;
// import oracle.jdbc.internal.OracleTypes;
// import oracle.jdbc.internal.OracleRowId;
// import oracle.sql.NUMBER;
import org.slf4j.Logger;
//import net.plumbing.msgbus.common.XMLchars;

//import javax.validation.constraints.NotNull;
//import java.io.ByteArrayInputStream;
//import java.io.ObjectInputStream;
//import java.math.BigInteger;
import java.sql.*;

//import static ClientIpHelper.toCamelCase;
import static net.plumbing.msgbus.common.XMLchars.*;


public class TheadDataAccess {
    private final int maxReasonLen =1996;
    public  Connection  Hermes_Connection=null;
    public PreparedStatement stmtMsgQueueDet=null;
    public PreparedStatement stmtMsgQueue=null;
    public PreparedStatement stmtMsgQueueConfirmationDet=null;
    public PreparedStatement stmtMsgQueueConfirmationTag = null;
    // private PreparedStatement stmtMsgQueueBody = null;
    //private PreparedStatement stmtMsgLastBodyTag = null;
    // private PreparedStatement stmtMsgQueueConfirmation ;
    private PreparedStatement stmt_UPDATE_MessageQueue_In2Ok = null;
    private  String dbSchema="orm";
    public String rdbmsVendor;


    // public void setDbSchema(String DbSchema) {
    //     this.dbSchema = DbSchema;
    // }
    public String getRdbmsVendor() {
        return rdbmsVendor;
    }
    public String INSERT_Message_Queue;

    private String UPDATE_MessageQueue_In2Ok;

    // private PreparedStatement stmt_UPDATE_MessageQueue_Temp2ErrIN;

    /*
    public PreparedStatement stmtUPDATE_MessageQueue_Queue_Date4Send;
    // HE-5481  Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    public final String UPDATE_MessageQueue_Queue_Date4Send =
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Date = sysdate, q.Queue_Direction = 'SEND'" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count=1 " +
                    ", q.Prev_Queue_Direction='OUT', q.Prev_Msg_Date=sysdate " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmtUPDATE_MessageQueue_Out2Send;
    // HE-5481  q.Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    public final String UPDATE_MessageQueue_Out2Send =
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
            "set q.Queue_Date = sysdate, q.Queue_Direction = 'SEND', q.Msg_Reason = ?" +
            ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count=1 " +
            ", q.Prev_Queue_Direction='OUT', q.Prev_Msg_Date=sysdate " +
            "where 1=1 and q.Queue_Id = ?  ";
    */
    private PreparedStatement stmt_UPDATE_Message_In2ExeIn=null;
    private  String UPDATE_MessageQueue_In2ExeIn;

    private PreparedStatement stmtUPDATE_MessageQueue_ExeIn2DelIN=null;
    private String UPDATE_MessageQueue_ExeIn2DelIN;

    private PreparedStatement stmtUPDATE_MessageQueue_In2ErrorIN=null;
    private  String UPDATE_MessageQueue_In2ErrorIN;

    private PreparedStatement stmtUPDATE_MessageQueue_ExeIN2PostIN;
    private String UPDATE_MessageQueue_ExeIN2PostIN;


    // private PreparedStatement stmtUPDATE_MessageQueue_Send2ErrorOUT;
    private String UPDATE_MessageQueue_Send2ErrorOUT;
/*
    public final String UPDATE_MessageQueue_SetMsg_Reason=
            "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'RESOUT', q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ? ";
    public PreparedStatement stmtUPDATE_MessageQueue_SetMsg_Reason;

    public PreparedStatement stmtUPDATE_MessageQueue_Send2AttOUT;
    public final String UPDATE_MessageQueue_Send2AttOUT=
            "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'ATTOUT', q.Msg_Result = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmt_UPDATE_MessageQueue_Send2finishedOUT;
    public final String UPDATE_MessageQueue_Send2finishedOUT=
            "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = ?, q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";

    private PreparedStatement stmt_UPDATE_MessageQueue_DirectionAsIS;
    private final String UPDATE_MessageQueue_DirectionAsIS=
            "update " + dbSchema + ".MESSAGE_QUEUE " +
                    "set Msg_Date= (current_timestamp + ? * interval '1' second), Msg_Reason = ?, Msg_Status = ?, Retry_Count= ?, " +
                    " Prev_Msg_Date=Msg_Date " +
                    "where 1=1 and Queue_Id = ?";

    public PreparedStatement stmt_UPDATE_MessageQueue_after_FaultResponse;
    public PreparedStatement stmt_UPDATE_after_FaultGet;

    public PreparedStatement stmt_Query_Message_Confirmation;
    public final String SELECT_Message_Confirmation= "select from " + dbSchema + ".MESSAGE_QueueDET D where D.queue_id =? and d.Tag_num >= ?";

    private final String SELECT_QUEUElog_Response="select Response from " + dbSchema + ".MESSAGE_QUEUElog where  ROWID = ?";
    private PreparedStatement stmt_SELECT_QUEUElog_Response;


*/


    // private PreparedStatement stmt_DELETE_Message_Details;
    // private String DELETE_Message_Details;

    private PreparedStatement stmt_DELETE_Message_Confirmation;
    private String DELETE_Message_Confirmation;
    private PreparedStatement stmt_DELETE_Message_ConfirmationH;
    private String DELETE_Message_ConfirmationH;

    public String SELECT_Link_Queue_Id ;
    public PreparedStatement stmt_SELECT_Link_Queue_Id;

    public PreparedStatement stmt_INSERT_Message_Details;
    public String INSERT_Message_Details ;

    private String UPDATE_QUEUElog_Response;
    private PreparedStatement stmt_UPDATE_QUEUElog;


    // TODO 4_Postgre
     private  String INSERT_QUEUElog_Request;
    // private PreparedStatement stmt_INSERT_QUEUElog;
    // TODO Для Oracle используется call insert into returning ROWID into
    public CallableStatement stmt_INSERT_QUEUElog;
    //private final String INSERT_QUEUElog_Request="{call insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, systimestamp, ?) returning ROWID into ? }";

    public  String selectMessageStatement;

    public PreparedStatement stmt_New_Queue_Prepare;
    /*
    public final String INSERT_Message_Queue= "INSERT into " + dbSchema + ".MESSAGE_Queue " +
                   "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) " +
            "values (?,        '"+ DirectNEWIN +"',          current_timestamp,    0,          current_timestamp,  0,            0,          'Undefine')";
     */
    public PreparedStatement stmt_New_Queue_Insert;

/*
    public void close_Hermes_Connection() {
        try { this.Hermes_Connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */

    public Connection make_Hikari_Connection_Only( String db_userid , String db_password,
                                                 HikariDataSource dataSource,
                                                 Logger dataAccess_log) {
        Connection Target_Connection ;
        String connectionUrl= dataSource.getJdbcUrl() ;
        // попробуй ARTX_PROJ / rIYmcN38St5P  || hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        if (connectionUrl.indexOf("oracle") > 0) {
            rdbmsVendor = "oracle";
        } else {
            rdbmsVendor = "postgresql";
        }
        dataAccess_log.info( "Try(thead) MessegeDB getConnection: " + connectionUrl + " as " + db_userid );

        try {
            Hermes_Connection = dataSource.getConnection();
            Hermes_Connection.setAutoCommit(false);

            if (!rdbmsVendor.equals("oracle")) {
                PreparedStatement stmt_SetTimeZone = Hermes_Connection.prepareStatement("set SESSION time zone 3");//.nativeSQL( "set SESSION time zone 3" );
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
            }

        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );
        Target_Connection = Hermes_Connection;
        return Target_Connection;
    }

    public Connection make_Hikari_Connection(  String HrmsSchema,
                                                String db_userid ,
                                                 HikariDataSource dataSource,
                                                 Logger dataAccess_log) {
        Connection Target_Connection ;
        String connectionUrl= dataSource.getJdbcUrl() ;
        // попробуй ARTX_PROJ / rIYmcN38St5P  || hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        this.dbSchema = HrmsSchema;
        if (connectionUrl.indexOf("oracle") > 0) {
            rdbmsVendor = "oracle";
        } else {
            rdbmsVendor = "postgresql";
        }
        dataAccess_log.info( "Try(thead) Hermes getConnection: " + connectionUrl + " as " + db_userid + " rdbmsVendor=" + rdbmsVendor);

        if ( Hermes_Connection == null)
        try {
        Hermes_Connection = dataSource.getConnection();
        Hermes_Connection.setAutoCommit(false);
        } catch (SQLException e) {
            dataAccess_log.error( "make_Hikari_Connection: `" + connectionUrl + "` fault:" + e.getMessage() );
            if ( Hermes_Connection != null)
                try {
                    Hermes_Connection.close();
                } catch ( SQLException SQLe) {
                    dataAccess_log.error( "make_Hikari_Connection close() for : `" + connectionUrl + "` fault:" + e.getMessage() );
                }
            e.printStackTrace();
            return (  null );
        }
        // dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );
        Target_Connection = Hermes_Connection;

        if (!rdbmsVendor.equals("oracle")) {
          try {
            PreparedStatement stmt_SetTimeZone = Hermes_Connection.prepareStatement("set SESSION time zone 3");//.nativeSQL( "set SESSION time zone 3" );
            stmt_SetTimeZone.execute();
            stmt_SetTimeZone.close();
        }catch (SQLException e) {
              dataAccess_log.error("make_Hikari_Connection stmt_SetTimeZone PreparedStatement for: `" + connectionUrl + "` fault:" + e.getMessage() );
              e.printStackTrace();
              if ( Hermes_Connection != null)
                  try {
                      Hermes_Connection.close();
                  } catch ( SQLException SQLe) {
                      dataAccess_log.error( "make_Hikari_Connection close() for : `" + connectionUrl + "` fault:" + e.getMessage() );
                  }
              return ( null);
          }
    }

        if (make_SelectNew_Queue(  dataAccess_log) == null ) {
            dataAccess_log.error( "make_SelectNew_Queue() fault");
            if ( Hermes_Connection != null)
                try {
                    Hermes_Connection.close();
                } catch ( SQLException SQLe) {
                    dataAccess_log.error( "make_Hikari_Connection close() for : `" + connectionUrl + "` fault:" + SQLe.getMessage() );
                }
            return null;
        }

        if (  make_Message_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Query() fault");
            return null;
        }

        if (  make_Message_QueryConfirmation(dataAccess_log) == null ) {
               dataAccess_log.error( "make_Message_QueryConfirmation() fault");
               return null;
           }

        if (  make_MessageDet_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_MessageDet_Query() fault");
            return null;
        }

        if (  make_Message_ConfirmationTag_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_ConfirmationTag_Query() fault");
            return null;
        }

        if (  make_update_MESSAGE_Template(dataAccess_log) == null ) {
            dataAccess_log.error( "make_update_MESSAGE_Template() fault");
            return null;
        }

/*
        if (  make_MessageConfirmation_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_MessageConfirmation_Query() fault");
            return null;
        }
        */

        if (  make_Message_Update_In2ExeIn(dataAccess_log) == null ) {
            dataAccess_log.error( "make_MessageQueue_ExeIn2DelIN() fault");
            return null;
        }

        if (  make_Message_Update_ExeIN2PostIN(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_ExeIN2PostIN() fault");
            return null;
        }
/*
        if (  make_delete_Message_Details(dataAccess_log) == null ) {
            dataAccess_log.error( "make_delete_Message_Details() fault");
            return null;
        }
        */

        if (  make_insert_Message_Details(dataAccess_log) == null ) {
            dataAccess_log.error( "make_insert_Message_Details() fault");
            return null;
        }
/*
        if ( make_Message_Update_Send2ErrorOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_Send2ErrorOUT() fault");
            return null;
        }

        if ( make_UPDATE_MessageQueue_Send2finishedOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }
        */

        /*
        if ( make_UPDATE_MessageQueue_Send2AttOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }
        */
        if ( make_DELETE_Message_Confirmation(dataAccess_log) == null ) {
            dataAccess_log.error( "make_DELETE_Message_Confirmation() fault");
            return null;
        }



//        if ( make_UPDATE_MessageQueue_DirectionAsIS(dataAccess_log) == null ) {
//            dataAccess_log.error( "make_UPDATE_MessageQueue_DirectionAsIS() fault");
//            return null;
//        }

        if ( make_UPDATE_QUEUElog(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_QUEUElog() fault");
            return null;
        }

        if ( make_INSERT_QUEUElog(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_QUEUElog() fault");
            return null;
        }

        /*if ( make_UPDATE_MessageQueue_SetMsg_Reason(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }*/
        /*
        if ( make_Message_Update_Queue_Queue_Date4Send( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }
        */

        if (make_insert_Message_Queue( dataAccess_log) == null ) {
            dataAccess_log.error( "make_insert_Message_Queue() fault");
            return null;
        }

        if ( make_Message_Update_In2ErrorIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_In2ErrorIN() fault");
            return null;
        }
        if ( make_UPDATE_MessageQueue_In2Ok( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_In2Ok() fault");
            return null;
        }
        /*
        if ( make_UPDATE_MessageQueue_Temp2ErrIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Temp2ErrIN() fault");
            return null;
        }
        */
        if ( make_Message_Update_ExeIn2DelIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_ExeIn2DelIN() fault");
            return null;
        }

        if ( make_SelectLink_Queue_Id( dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_ExeIn2DelIN() fault");
            return null;
        }

        if ( make_delete_Message_Details( dataAccess_log) == null ) {
            dataAccess_log.error( "make_delete_Message_Details() fault");
            return null;
        }

        if (make_Message_LastBodyTag_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_LastBodyTag_Query() fault");
            return null;
        }
        return Target_Connection;
    }

    public PreparedStatement stmt_DELETE_Message_Details;
    public  String DELETE_Message_Details;

    private PreparedStatement  make_delete_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        DELETE_Message_Details = "delete from " + dbSchema + ".MESSAGE_QueueDET D where D.queue_id =?";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Details );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    private PreparedStatement make_SelectLink_Queue_Id( Logger dataAccess_log )
    {
        PreparedStatement StmtMsg_Queue;
        try {

            SELECT_Link_Queue_Id = "select Link_Queue_Id, Msg_Reason from " + dbSchema + ".MESSAGE_Queue where Queue_Id=?";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( SELECT_Link_Queue_Id);
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }

        this.stmt_SELECT_Link_Queue_Id = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    private PreparedStatement make_SelectNew_Queue(  Logger dataAccess_log )
    {
        PreparedStatement StmtMsg_Queue;
        try {
            if ( rdbmsVendor.equals("oracle") )
                selectMessageStatement = "select " + dbSchema + ".MESSAGE_QUEUE_SEQ.NEXTVAL as queue_id," +
                        //"select nextval('" + dbSchema + ".MESSAGE_QUEUE_SEQ') as queue_id," +
                        " '" + DirectNEWIN +"' as queue_direction," +
                        " current_timestamp  Queue_Date," +
                        " 0 msg_status," +
                        " current_timestamp Msg_Date," +
                        " 0 operation_id," +
                        " 0 outqueue_id," +
                        " 'Undefine' as msg_type," +
                        " NULL as  msg_reason," +
                        " 0 msgdirection_id," +
                        " 100001 msg_infostreamid," +
                        " NULL msg_type_own," +
                        " NULL msg_result," +
                        " NULL subsys_cod," +
                        " 0 as Retry_Count," +
                        " NULL prev_queue_direction," +
                        " current_timestamp Prev_Msg_Date, " +
                        " current_timestamp Queue_Create_Date " +
                        "from DUAL "  ;
            else selectMessageStatement = "select nextval('" + dbSchema + ".MESSAGE_QUEUE_SEQ') as queue_id," +
                    " '" + DirectNEWIN +"' as queue_direction," +
                    " current_timestamp  Queue_Date," +
                    " 0 msg_status," +
                    " current_timestamp Msg_Date," +
                    " 0 operation_id," +
                    " 0 outqueue_id," +
                    " 'Undefine' as msg_type," +
                    " NULL as  msg_reason," +
                    " 0 msgdirection_id," +
                    " 100001 msg_infostreamid," +
                    " NULL msg_type_own," +
                    " NULL msg_result," +
                    " NULL subsys_cod," +
                    " 0 as Retry_Count," +
                    " NULL prev_queue_direction," +
                    " current_timestamp Prev_Msg_Date, " +
                    " current_timestamp Queue_Create_Date ";

        //dataAccess_log.info( "MESSAGE_QueueSelect4insert:" + selectMessageStatement );
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( selectMessageStatement);
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }

        this.stmt_New_Queue_Prepare = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    private PreparedStatement  make_insert_Message_Queue( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
             INSERT_Message_Queue= "INSERT into " + dbSchema + ".MESSAGE_Queue " +
                    "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) " +
                    "values (?,        '"+ DirectNEWIN +"',          current_timestamp,    0,          current_timestamp,  0,            0,          'Undefine')";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Queue );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_New_Queue_Insert = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }


    private PreparedStatement  make_UPDATE_QUEUElog( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_QUEUElog_Response="update " + dbSchema + ".MESSAGE_QUEUElog set Resp_DT = current_timestamp, Response = ? where QUEUE_ID= ? and ROWID = ?";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( UPDATE_QUEUElog_Response );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }

        this.stmt_UPDATE_QUEUElog = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
    //public final String UPDATE_QUEUElog_Response="update " + dbSchema + ".MESSAGE_QUEUElog L set l.Resp_DT = current_timestamp, l.Response = ? where l.Queue_Id = ?";
    //public PreparedStatement stmt_UPDATE_QUEUElog;

    public int doUPDATE_QUEUElog( String ROWID_QUEUElog, // TODO RowId ROWID_QUEUElog, (oracle)
                                  long Queue_Id, String sResponse,
                                                       Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_QUEUElog: `update " + dbSchema + ".MESSAGE_QUEUElog L set l.Resp_DT = current_timestamp, l.Response = '" +sResponse +
                "' " + "where l.Queue_Id = "+ Queue_Id +" and ROWID = '" + ROWID_QUEUElog + "' ;`" );
        try {
           // TODO for Postgree !!!
            stmt_UPDATE_QUEUElog.setString( 3,  ROWID_QUEUElog );
            //stmt_UPDATE_QUEUElog.setRowId( 3,ROWID_QUEUElog);
            //stmt_UPDATE_QUEUElog.setString( 3,ROWID_QUEUElog);
            stmt_UPDATE_QUEUElog.setLong( 2, Queue_Id );
            stmt_UPDATE_QUEUElog.setString( 1, sResponse );
            stmt_UPDATE_QUEUElog.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + UPDATE_QUEUElog_Response + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
/* It's for ORACLE !!!
    public CallableStatement  make_INSERT_QUEUElog( Logger dataAccess_log ) {
        CallableStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareCall( INSERT_QUEUElog_Request );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (CallableStatement) null );
        }
        //this.stmt_INSERT_QUEUElog = (OraclePreparedStatement)StmtMsg_Queue;
        this.stmt_INSERT_QUEUElog = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
    -------------------------------------*/
 private PreparedStatement  make_INSERT_QUEUElog( Logger dataAccess_log ) {
    if (!rdbmsVendor.equals("oracle")) {
        // TODO 4_Postgre
        PreparedStatement StmtMsg_Queue;
        INSERT_QUEUElog_Request="insert into " + dbSchema + ".MESSAGE_QUEUElog  ( Queue_Id, Req_dt, RowId, Request ) values( ?, current_timestamp, cast(nextval( '"+ dbSchema + ".message_queuelog_seq') as varchar), ?) ";
        try {  StmtMsg_Queue = this.Hermes_Connection.prepareCall(INSERT_QUEUElog_Request);
        } catch (Exception e) {
            dataAccess_log.error(e.getMessage());
            e.printStackTrace();
            return ((PreparedStatement) null);
        }
        this.stmt_INSERT_QUEUElog = (CallableStatement)StmtMsg_Queue;
        // TODO RowId Postgree RowId
        // this.stmt_INSERT_QUEUElog = StmtMsg_Queue;
        return StmtMsg_Queue;
    }
    else
    { CallableStatement StmtMsg_Queue;
        INSERT_QUEUElog_Request = "{call insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, systimestamp, ?) returning ROWID into ? }";
        try {  StmtMsg_Queue = this.Hermes_Connection.prepareCall(INSERT_QUEUElog_Request);
        } catch (Exception e) {
            dataAccess_log.error(e.getMessage());
            e.printStackTrace();
            return ((CallableStatement) null);
        }
        this.stmt_INSERT_QUEUElog = (CallableStatement)StmtMsg_Queue;
        // TODO RowId Oracle native RowId
        return StmtMsg_Queue;
    }
}
    //public final String INSERT_QUEUElog_Request="insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, current_timestamp, ?)";
    // public PreparedStatement stmt_INSERT_QUEUElog;
    /*
    public  RowId doINSERT_QUEUElog(long Queue_Id, String sRequest,
                                  Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] do {call insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values(" + Queue_Id + ", current_timestamp, '"+sRequest+ "' ) returning ROWID into ? };" );
        int count ;
        RowId ROWID_QUEUElog=null;
        try {
            stmt_INSERT_QUEUElog.setLong( 1, Queue_Id );
            stmt_INSERT_QUEUElog.setString( 2, sRequest );
            stmt_INSERT_QUEUElog.registerOutParameter( 3, Types.ROWID );
            // stmt_INSERT_QUEUElog.re  // .registerReturnParameter(3, OracleTypes.ROWID);
            count = stmt_INSERT_QUEUElog.executeUpdate();
            if (count>0)
            {
              //  ROWID_QUEUElog = stmt_INSERT_QUEUElog.getRowId(4);
                ROWID_QUEUElog  = stmt_INSERT_QUEUElog.getRowId(3); //rest is not null and not empty
            }
            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "insert into " + dbSchema + ".MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + INSERT_QUEUElog_Request + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ROWID_QUEUElog;
        }
        return ROWID_QUEUElog;
    }
*/
    public  String // TODO RowId Postgree
            doINSERT_QUEUElog(long Queue_Id, String sRequest,
                                     Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] " + INSERT_QUEUElog_Request + " Queue_Id=" + Queue_Id + ", Request='"+sRequest+ "' " );
        int count ;
        String ROWID_QUEUElog=null;
        try {
            stmt_INSERT_QUEUElog.setLong( 1, Queue_Id );
            stmt_INSERT_QUEUElog.setString( 2, sRequest );
            // TODO for Oracle ROWID, в случае Postgree комментим !!!
            if ( rdbmsVendor.equals("oracle") )
                stmt_INSERT_QUEUElog.registerOutParameter( 3, Types.ROWID );
            count = stmt_INSERT_QUEUElog.executeUpdate();
            // dataAccess_log.info( "[" + Queue_Id + "] count = stmt_INSERT_QUEUElog.executeUpdate() => " + count);
            if (count>0)
            {
                // TODO for Oracle ROWID, в случае Postgree комментим !!!
                if ( rdbmsVendor.equals("oracle") )
                    ROWID_QUEUElog  = stmt_INSERT_QUEUElog.getString(3); //rest is not null and not empty

                else { // TODO RowId Postgree
                    Statement stmt = Hermes_Connection.createStatement();

                    // get the postgresql serial field value with this query
                    ResultSet rs = stmt.executeQuery("select cast(currval('message_queuelog_seq')as varchar)");
                    if (rs.next()) {
                        ROWID_QUEUElog = rs.getString(1);
                    }
                    stmt.close();
                }
                dataAccess_log.info( "[" + Queue_Id + "] ROWID = stmt_INSERT_QUEUElog.executeUpdate() => " + ROWID_QUEUElog);
            }
            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "insert into " + dbSchema + ".MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + INSERT_QUEUElog_Request + ") fault: " + e.getMessage() );
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + INSERT_QUEUElog_Request + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + INSERT_QUEUElog_Request + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return null;
        }
        /*
        RowId myRowId= null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream( ROWID_QUEUElog.getBytes() );
            ObjectInputStream objectInputStream = new ObjectInputStream(
                    in);
            myRowId = (RowId) objectInputStream.readObject();

        } catch (Exception e) {
            dataAccess_log.error( "Cust RowId from '" + ROWID_QUEUElog + "' for [" + Queue_Id +  "]: " + INSERT_QUEUElog_Request + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return null;
        }

        return myRowId;
        */
        return ROWID_QUEUElog;
    }

    private PreparedStatement  make_insert_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            INSERT_Message_Details= "INSERT into " + dbSchema + ".MESSAGE_QueueDET (QUEUE_ID, TAG_ID, TAG_VALUE, TAG_NUM, TAG_PAR_NUM) " +
                    "values (?, ?, ?, ?, ?)";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Details );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_INSERT_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
/*
    public PreparedStatement  make_UPDATE_MessageQueue_Temp2ErrIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = '" + XMLchars.DirectERRIN +"'" +
                            ", Queue_Date= current_timestamp" +
                            ", Msg_Status = 0" +
                            ", Msg_Date= current_timestamp" +
                            ", Operation_Id=?" +
                            ", Outqueue_Id=? " +
                            ", msg_type = ?" +
                            ", Msg_Reason = ?" +
                            ", MsgDirection_Id= ?" +
                            ", msg_infostreamid = 0" +
                            ", msg_type_own = ?" +
                            ", msg_result = null" +
                            ", subsys_cod = ?" +
                            ", Retry_Count=0 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", Prev_Queue_Direction='+" + DirectNEWIN + "'" +
                            ", Prev_Msg_Date=Msg_Date " +
                            "where 1=1 and Queue_Id = ?  " );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmt_UPDATE_MessageQueue_Temp2ErrIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
    */

    public  int doUPDATE_MessageQueue_Temp2ErrIN(Long Queue_Id, Integer Operation_Id,
                                                 Integer MsgDirection_Id , String SubSys_Cod,
                                                 String Msg_Type, String Msg_Type_own,
                                                 String Msg_Reason, Long OutQueue_Id,
                                                 Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_In: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
        //        "set q.Queue_Direction = 'IN', q.Msg_Reason = '"+ Msg_Reason+ "' " +
        //        ", q.Msg_Date= current_timestamp,  q.Msg_Status = 0, q.Retry_Count= 1 " +
        //        ", q.Prev_Queue_Direction='TMP', q.Prev_Msg_Date=q.Msg_Date " +
        // "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 1, Operation_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 2, OutQueue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 3, Msg_Type );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 4, Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason );
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 5, MsgDirection_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 6, Msg_Type_own );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 7, SubSys_Cod );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 8, Queue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {
            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]:  " + UPDATE_MessageQueue_In2Ok + " ) fault: " + e.getMessage() );
            System.err.println( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_In2Ok + " ) fault: ");
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_In2Ok + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_In2Ok + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    private PreparedStatement  make_UPDATE_MessageQueue_In2Ok( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_In2Ok =
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'IN'" +
                            ", Queue_Date= current_timestamp" +
                            ", Msg_Status = 0" +
                            ", Msg_Date= current_timestamp" +
                            ", Operation_Id=?" +
                            ", Outqueue_Id=? " +
                            ", msg_type = ?" +
                            ", Msg_Reason = ?" +
                            ", MsgDirection_Id= ?" +
                            ", msg_infostreamid = 0" +
                            ", msg_type_own = ?" +
                            ", msg_result = null" +
                            ", subsys_cod = ?" +
                            ", Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", Prev_Queue_Direction='TMP'" +
                            ", Prev_Msg_Date=Msg_Date " +
                            "where 1=1 and Queue_Id = ?  " ;
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_In2Ok );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmt_UPDATE_MessageQueue_In2Ok = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_In2Ok(Long Queue_Id, Integer Operation_Id,
                                            Integer MsgDirection_Id , String SubSys_Cod,
                                            String Msg_Type, String Msg_Type_own,
                                            String Msg_Reason, String OutQueue_Id,
                                            Logger dataAccess_log ) {
        // dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_In2Ok: " + UPDATE_MessageQueue_In2Ok + " {"+ Queue_Id +"} SubSys_Cod=`" + SubSys_Cod+ "`" );
        try {
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 1, Operation_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 2, Long.parseLong(OutQueue_Id) );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 3, Msg_Type );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 4, Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason );
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 5, MsgDirection_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 6, Msg_Type_own );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 7, SubSys_Cod );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 8, Queue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {
            dataAccess_log.error( "[" + Queue_Id + "]  " + UPDATE_MessageQueue_In2Ok + " for [" + Queue_Id+  "]:  doUPDATE_MessageQueue_In2Ok ) fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "]  " + UPDATE_MessageQueue_In2Ok + " for [" + Queue_Id+  "]: doUPDATE_MessageQueue_In2Ok )) fault: ");
            e.printStackTrace();
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_In2Ok + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_In2Ok + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    private PreparedStatement  make_Message_Update_In2ExeIn( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_In2ExeIn=
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'EXEIN'" +
                            ", Msg_Date= current_timestamp, Msg_Reason = ?" +
                            ", Prev_Queue_Direction= Queue_Direction, Prev_Msg_Date=Msg_Date " +
                            "where 1=1 and Queue_Id = ?  ";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_In2ExeIn );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_Message_In2ExeIn = StmtMsg_Queue;

        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_IN2ExeIN(Long Queue_Id,
                                                       String pMsg_Reason,
                                                       Logger dataAccess_log ) {
//        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_IN2ExeIN: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
//                "set  q.Msg_Reason = '"+ pMsg_Reason+ "' " +
//                ", q.Msg_Date= current_timestamp,  " +
//                ", q.Prev_Queue_Direction='IN', q.Prev_Msg_Date=q.Msg_Date " +
//                "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_Message_In2ExeIn.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
             stmt_UPDATE_Message_In2ExeIn.setLong( 2, Queue_Id );
            stmt_UPDATE_Message_In2ExeIn.executeUpdate();
            Hermes_Connection.commit();
            // dataAccess.do_Commit();
        } catch (Exception e) {
            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_In2ExeIn + ") fault: " + e.getMessage() );
            System.err.println( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_In2ExeIn + ") fault: ");
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_In2ExeIn + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_In2ExeIn + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
/*
    public PreparedStatement  make_UPDATE_MessageQueue_DirectionAsIS( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_DirectionAsIS );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_DirectionAsIS = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_DirectionAsIS(Long Queue_Id, int Retry_interval,
                                                       String pMsg_Reason,
                                                       int Msg_Status, int Retry_Count,
                                                       Logger dataAccess_log ) {
        dataAccess_log.info("[" + Queue_Id + "] try UPDATE_MessageQueue_DirectionAsIS : ["+ UPDATE_MessageQueue_DirectionAsIS + "]" );

        try {
            BigDecimal queueId = new BigDecimal( Queue_Id.toString() );
            dataAccess_log.info("[" + Queue_Id + "] BigDecimal queueId =" + queueId.toString() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 1, Retry_interval );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setString( 2, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 3, Msg_Status );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 4, Retry_Count );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setBigDecimal(5, queueId );
                    // NUMBER.formattedTextToNumber( Queue_Id.toString())  ) ; //.setNUMBER( 5, NUMBER.formattedTextToNumber() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: " + e.getMessage() );
            System.err.println( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
*/
private PreparedStatement  make_DELETE_Message_Confirmation( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        PreparedStatement StmtMsg_QueueH;
        try {
            DELETE_Message_Confirmation= "delete from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_par_num >=" +
                    "(select min(d.tag_num) from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ? and d.tag_id='Confirmation')";
            DELETE_Message_ConfirmationH= "delete from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_id='Confirmation'";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Confirmation );
            StmtMsg_QueueH = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_ConfirmationH );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Confirmation = StmtMsg_Queue;
        this.stmt_DELETE_Message_ConfirmationH = StmtMsg_QueueH;
        return  StmtMsg_Queue ;
    }

    public  int doDELETE_Message_Confirmation(long Queue_Id, Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doDELETE_Message_ConfirmationBody! {};", DELETE_Message_Confirmation );
        try {
                // сначала удаляем всЁ, что растет из Confirmation
            stmt_DELETE_Message_Confirmation.setLong( 1, Queue_Id );
            stmt_DELETE_Message_Confirmation.setLong( 2, Queue_Id );
            stmt_DELETE_Message_Confirmation.executeUpdate();
                 // а теперь и сам Confirmation tag
            dataAccess_log.info( "[" + Queue_Id + "] doDELETE_Message_ConfirmationTag {};", DELETE_Message_ConfirmationH );
            stmt_DELETE_Message_ConfirmationH.setLong( 1, Queue_Id );
            stmt_DELETE_Message_ConfirmationH.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {
            dataAccess_log.error( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: " + e.getMessage() );
            System.err.println( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: ");
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + DELETE_Message_Confirmation + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + DELETE_Message_Confirmation + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
/*
    public PreparedStatement  make_delete_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            DELETE_Message_Details= "delete from " + dbSchema + ".MESSAGE_QueueDET D where D.queue_id =?";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Details );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
*/

/*
    public PreparedStatement  make_Message_Update_Queue_Queue_Date4Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Queue_Date4Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Queue_Date4Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_Message_Update_Out2Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Out2Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Out2Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int  doUPDATE_MessageQueue_Queue_Date4Send( long Queue_Id,   Logger dataAccess_log ) {
        dataAccess_log.info("[" + Queue_Id + "] doUPDATE_MessageQueue_Queue_Date4Send()" );
        try {

            stmtUPDATE_MessageQueue_Queue_Date4Send.setLong( 1, Queue_Id );
            stmtUPDATE_MessageQueue_Queue_Date4Send.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;

    }

    public  int doUPDATE_MessageQueue_Out2Send( long Queue_Id,  String pMsg_Reason, Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_Out2Send:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Out2Send.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Out2Send.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_Out2Send.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " );
                    e.printStackTrace();
            return -1;
        }
        return 0;
    }
*/
/*
    public PreparedStatement  make_Message_Update_Send2ErrorOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_Send2ErrorOUT=
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'ERROUT', Msg_Reason = ?" +
                            ", Msg_Date= current_timestamp,  Msg_Status = ?, Retry_Count= ? " +
                            ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                            "where Queue_Id = ? ";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2ErrorOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2ErrorOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Send2ErrorOUT(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setString( 1,  pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason  );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public PreparedStatement  make_UPDATE_MessageQueue_Send2AttOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2AttOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2AttOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Send2AttOUT(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Send2AttOUT.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2AttOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2AttOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
    */

    /*
    public PreparedStatement  make_UPDATE_MessageQueue_SetMsg_Reason( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_SetMsg_Reason );
        } catch (SQLException e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_SetMsg_Reason = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_SetMsg_Reason(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_SetMsg_Reason.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_SetMsg_Reason.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
*/

    private PreparedStatement  make_Message_Update_ExeIn2DelIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_ExeIn2DelIN =
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'DELIN'" +
                            ", Msg_Date= current_timestamp" +
                            ", Prev_Queue_Direction= Queue_Direction, Prev_Msg_Date=Msg_Date " +
                            "where Queue_Id = ?  ";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_ExeIn2DelIN );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_ExeIn2DelIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_ExeIn2DelIN(Long Queue_Id, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_ExeIn2DelIN:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_ExeIn2DelIN.setLong( 1, Queue_Id );
            stmtUPDATE_MessageQueue_ExeIn2DelIN.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " );
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    private PreparedStatement  make_Message_Update_In2ErrorIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_In2ErrorIN= "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'ERRIN', Msg_Reason = ?" +
                            ", Msg_Date= current_timestamp, Msg_Status = ?, Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", Prev_Queue_Direction= Queue_Direction, Prev_Msg_Date=Msg_Date " +
                            "where Queue_Id = ?  ";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_In2ErrorIN );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_In2ErrorIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_In2ErrorIN(Long Queue_Id, String pMsg_Reason, Integer pMsg_Status, Logger dataAccess_log) {
         // dataAccess_log.warn( "["+ Queue_Id + "] doUPDATE_MessageQueue_Out2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_In2ErrorIN.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_In2ErrorIN.setInt( 2, pMsg_Status );
            stmtUPDATE_MessageQueue_In2ErrorIN.setLong( 3, Queue_Id );
            stmtUPDATE_MessageQueue_In2ErrorIN.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();
        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + e.getMessage() );
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    private PreparedStatement  make_Message_Update_ExeIN2PostIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_ExeIN2PostIN=
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = '"+ DirectPOSTIN +"', Msg_Reason = ?" +
                            ", Msg_Date= current_timestamp " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", Prev_Queue_Direction='" + DirectEXEIN + "', Prev_Msg_Date=Msg_Date " +
                            "where Queue_Id = ?  ";
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_ExeIN2PostIN );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_ExeIN2PostIN + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_ExeIN2PostIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_ExeIN2PostIN(Long Queue_Id, String pMsg_Reason, Logger dataAccess_log) {
         dataAccess_log.info( "doUPDATE_MessageQueue_ExeIN2PostIN:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_ExeIN2PostIN.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_ExeIN2PostIN.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_ExeIN2PostIN.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIN2PostIN + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIN2PostIN + ") fault: " );
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_ExeIN2PostIN + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_ExeIN2PostIN + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    private PreparedStatement  make_Message_QueryConfirmation( Logger dataAccess_log ) {
        PreparedStatement stmtMsgQueueConfirmationDet;
        try {

            stmtMsgQueueConfirmationDet = this.Hermes_Connection.prepareStatement(
        //"select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from artx_proj.message_queuedet D where (1=1)  and d.QUEUE_ID = ? and d.Tag_Num >= ? order by   Tag_Par_Num, Tag_Num "
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from "+ dbSchema +".message_QueueDet D where (1=1) and d.QUEUE_ID = ? and d.Tag_Num = ? " +
                            "union all " +
                            "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from "+ dbSchema +".message_QueueDet D where (1=1)  and d.QUEUE_ID = ? and d.Tag_Par_Num >= ? " +
                            "order by   4, 3"
        );

        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmationDet = stmtMsgQueueConfirmationDet;
        return  stmtMsgQueueConfirmationDet ;
    }

    public PreparedStatement stmtMsgLastBodyTag = null;
    public String selectMsgLastBodyTag = null;

    private PreparedStatement  make_Message_LastBodyTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;

            if (rdbmsVendor.equals("oracle")) {
                selectMsgLastBodyTag = "select Tag_Num from (" +
                        " select Tag_Num from (" +
                        " select Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation'" +
                        " union all" +
                        " select max(Tag_Num) + 1  as  Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ?" +
                        " ) order by Tag_Num" +
                        " ) where rownum =1";
            } else {
                selectMsgLastBodyTag = "select Tag_Num from (" +
                        " select Tag_Num from (" +
                        " select Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation'" +
                        " union all" +
                        " select max(Tag_Num) + 1  as  Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ?" +
                        " ) conf_union order by Tag_Num" +
                        " ) conf_tag limit 1";
            }
        try {
                StmtMsgQueueDet = (PreparedStatement) this.Hermes_Connection.prepareStatement(selectMsgLastBodyTag);

        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgLastBodyTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_Message_ConfirmationTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            if (rdbmsVendor.equals("oracle")) {
                StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                        "select Tag_Num from ( select Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation' order by Tag_Num desc) qd " +
                                "where rownum=1"
                );
            }
            else
            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                            "select Tag_Num from ( select Tag_Num from " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation' order by Tag_Num desc) qd " +
                                "limit 1"
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmationTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    private  String update_MESSAGE_Template;
    private PreparedStatement stmt_update_MESSAGE_Template;
    private  PreparedStatement make_update_MESSAGE_Template( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        update_MESSAGE_Template = "update " + dbSchema +  ".Message_Templates set conf_text=?, Lastmaker=?, Lastdate=current_timestamp where Template_Id =?";
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(update_MESSAGE_Template);
        } catch (SQLException e) {
            dataAccess_log.error( "{} fault: {}", update_MESSAGE_Template, e.getMessage());
            e.printStackTrace();
            return ((PreparedStatement) null);
        }
        stmt_update_MESSAGE_Template = StmtMsg_Queue;
        return StmtMsg_Queue;
    }
    public  int doUpdate_MESSAGE_Template( long Queue_Id, int updatedTemplate_Id, String Conf_Text , Logger dataAccess_log) {

        if (stmt_update_MESSAGE_Template != null) {
            try {
                stmt_update_MESSAGE_Template.setString(1, Conf_Text );
                String LastMaker = "j." + System.getenv("LOGNAME");
                stmt_update_MESSAGE_Template.setString(2, LastMaker );
                stmt_update_MESSAGE_Template.setInt(3, updatedTemplate_Id);
                stmt_update_MESSAGE_Template.executeUpdate();
                dataAccess_log.warn( "["+ Queue_Id +" ]>" + update_MESSAGE_Template + ":Template_Id=[" + updatedTemplate_Id + "] done");

                this.Hermes_Connection.commit();
            } catch (SQLException e) {
                System.err.println(">" + update_MESSAGE_Template + ":Template_Id=[" + updatedTemplate_Id + "] :" + e.getMessage());
                e.printStackTrace();
                dataAccess_log.error( "["+ Queue_Id +" ] doUpdate_MESSAGE_Template for [" + updatedTemplate_Id + "] (" + update_MESSAGE_Template + ") fault: " + e.getMessage() );

                try {
                    this.Hermes_Connection.rollback();
                } catch (SQLException exp) {
                    dataAccess_log.error("["+ Queue_Id +" ] Connection.rollback() fault: " + exp.getMessage());
                }
                return -11;
            }
        }
        return 0;
    }

/*
    public PreparedStatement  make_MessageConfirmation_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            StmtMsgQueueDet = this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from " + dbSchema + ".message_queuedet D" +
                            " where (1=1)" +
                            " and d.QUEUE_ID = ? and d.Tag_Id >= ? " +
                            " order by   Tag_Par_Num, Tag_Num "
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmation = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }
*/
private PreparedStatement make_Message_Query(  Logger dataAccess_log ) {
    PreparedStatement stmtMsgQueue;

    String selectMessageStatement =
            "select Q.queue_direction," +
                    " Q.msg_status," +
                    " Q.outqueue_id," +
                    " Q.msg_reason," +
                    " Q.msg_result, " +
                    " Q.msg_infostreamid " +
                    "from " + dbSchema + ".MESSAGE_QUEUE q Where  Q.queue_id = ?";
    try {
        //dataAccess_log.info("MESSAGE_QueueSelect:" + selectMessageStatement);
        stmtMsgQueue = this.Hermes_Connection.prepareStatement(selectMessageStatement);


    } catch (Exception e) {
        dataAccess_log.error(e.getMessage());
        e.printStackTrace();
        return ((PreparedStatement) null);
    }
    this.stmtMsgQueue = stmtMsgQueue;
    return stmtMsgQueue;
    }

    private PreparedStatement  make_MessageDet_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from " + dbSchema + ".Message_QueueDet D where (1=1) and d.QUEUE_ID = ? order by d.Tag_Par_Num, d.Tag_Num"
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( null );
        }
        this.stmtMsgQueueDet = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }
}
