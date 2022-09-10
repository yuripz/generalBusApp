package net.plumbing.msgbus.monitoring;
import org.slf4j.Logger;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.springframework.beans.factory.annotation.Autowired;
//import DataAccess;


public class AppendDataAccess {
    public static Connection  Hermes_Connection;
    public  static Date InitDate;
    public static DateFormat dateFormat;

    @Autowired
    /*
    static JdbcTemplate jdbcTemplate;
    static DriverManagerDataSource dataSource;
    */
    private static PreparedStatement stmtInsertData;

    public static  Connection make_Monitoring_Connection(  String dataSourceClassName, String dst_point, String db_userid , String db_password, Logger dataAccess_log) {
        Connection Target_Connection = null;
        String connectionUrl ;
        if ( dst_point==null) {
            connectionUrl = "jdbc:oracle:thin:@//10.242.36.5:1521/hermes12"; // Test-Capsul !!!
            //connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
        }
        else {
            connectionUrl = dst_point;
        }
        // попробуй ARTX_PROJ / rIYmcN38St5P
        // hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";

        dataAccess_log.info( "Try monitoring getConnection: " + connectionUrl + " as " + db_userid );
        try {
            // Establish the connection.
            if ( dataSourceClassName != null )
                Class.forName( dataSourceClassName) ;
            else
                Class.forName("oracle.jdbc.driver.OracleDriver");

            Target_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
            // Handle any errors that may have occurred.
            Target_Connection.setAutoCommit(false);

            AppendDataAccess.Hermes_Connection = Target_Connection;
            dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
            stmtInsertData = AppendDataAccess.Hermes_Connection.prepareStatement(SQLInsertData );


        } catch (SQLException |ClassNotFoundException e) {
            e.printStackTrace();
            return ( (Connection) null );
        }

        dataAccess_log.info( "monitoring getConnection: " + connectionUrl + " as " + db_userid + "  done" );

        return Target_Connection;
    }


    private static final String SQLInsertData= "insert into TS_DWH.MESSAGE_QUEUE (\n" +
            "       queue_id,\n" +
            "       queue_direction,\n" +
            "       queue_date,\n" +
            "       msg_status,\n" +
            "       msg_date,\n" +
            "       operation_id,\n" +
            "       outqueue_id,\n" +
            "       msg_type,\n" +
            "       msg_reason,\n" +
            "       msgdirection_id,\n" +
            "       msg_infostreamid,\n" +
            "       msg_type_own,\n" +
            "       msg_result,\n" +
            "       subsys_cod,\n" +
            "       retry_count,\n" +
            "       prev_queue_direction,\n" +
            "       prev_msg_date,\n" +
            "       queue_create_date,\n" +
            "       perform_object_id,\n" +
            "       req_dt,\n" +
            "       request,\n" +
            "       resp_dt,\n" +
            "       response)\n" +
            "values(  ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?,\n" +
            "       ?\n" +
            ");";




}
