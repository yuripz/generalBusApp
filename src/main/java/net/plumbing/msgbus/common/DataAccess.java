package net.plumbing.msgbus.common;

import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

//import org.springframework.beans.factory.annotation.Autowired;

public  class DataAccess {

    public static Connection  Hermes_Connection;
    public  static Timestamp InitDate;
    // public  static Timestamp InitDateTime; ( для проверки наличия методов при отладке )
    public static DateFormat dateFormat;

    private static PreparedStatement stmtCurrentTimeStringRead;
    private static PreparedStatement stmtCurrentTimeDateRead;
    public static  String HrmsSchema="orm";
    public static  String rdbmsVendor="oracle";
    private static  String SQLCurrentTimeStringRead;
    private static  String SQLCurrentTimeDateRead;

    public static  Connection make_Hermes_Connection( String DbSchema,  Connection pTarget_Connection, String dst_point, String db_userid , String db_password, Logger dataAccess_log) {
        Connection Target_Connection = null;
        String connectionUrl ;
        if ( dst_point==null) {
            connectionUrl = "jdbc:oracle:thin:@//10.242.36.8:1521/hermes12"; // Test-Capsul !!!
            //connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
        }
        else {
            connectionUrl = dst_point;
        }
        // попробуй ARTX_PROJ / rIYmcN38St5P
        // hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        if ( connectionUrl.contains("oracle") ) rdbmsVendor="oracle";
        else rdbmsVendor="postgresql";
        HrmsSchema =  DbSchema;

        dataAccess_log.info( "Try DataBase getConnection: " + connectionUrl + " as " + db_userid + " to RDBMS " + rdbmsVendor + " DbSchema:" + DbSchema);

        try {
            if ( pTarget_Connection == null ) {
                // Establish the connection.
                Class.forName("oracle.jdbc.driver.OracleDriver");
                Target_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
                // Handle any errors that may have occurred.
            }
            else {
                dataAccess_log.info( "Try HikariDataSource [" + pTarget_Connection.getNetworkTimeout() +"] Connection: " + connectionUrl + " as " + db_userid );
                Target_Connection = pTarget_Connection;
            }

            Target_Connection.setAutoCommit(false);
            if ( !rdbmsVendor.equals("oracle") ) {
                SQLCurrentTimeStringRead= "SELECT to_char( clock_timestamp() at time zone 'Europe/Moscow', 'YYYYMMDDHH24MISS') as InitTime";
                SQLCurrentTimeDateRead= "SELECT clock_timestamp() at time zone 'Europe/Moscow' as InitTime";
                dataAccess_log.info("Try setup Connection: `set SESSION time zone 3; set enable_bitmapscan to off;`");
                PreparedStatement stmt_SetTimeZone = Target_Connection.prepareStatement("set SESSION time zone 3; set enable_bitmapscan to off;");//.nativeSQL( "set SESSION time zone 3" );
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
                Target_Connection.commit();
            }
            else
            { // используем DUAL
                SQLCurrentTimeStringRead= "SELECT to_char(current_timestamp, 'YYYYMMDDHH24MISS') as InitTime FROM dual";
                SQLCurrentTimeDateRead= "SELECT current_timestamp as InitTime FROM dual";
            }

            DataAccess.Hermes_Connection = Target_Connection;
            dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
            stmtCurrentTimeStringRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeStringRead );
            stmtCurrentTimeDateRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeDateRead );

            ResultSet rs = null;
            rs = stmtCurrentTimeDateRead.executeQuery();
            while (rs.next()) {
                InitDate = rs.getTimestamp("InitTime");
                // InitDateTime = rs.getTimestamp("InitTime");
                dataAccess_log.info( "RDBMS InitDate" + InitDate.toString() + " getTime="  + InitDate.getTime()  + " mSec., dateFormat:" + dateFormat.format( InitDate ) );
                // dataAccess_log.info( "RDBMS CurrentTime: LocalDate ="+ InitDate.toLocalDate().toString() + " getTime=" + InitDate.getTime()  + " mSec., " + dateFormat.format( InitDate )  );
            }
            rs.close();
            stmtCurrentTimeDateRead.close();
            stmtCurrentTimeDateRead = null;
            DataAccess.Hermes_Connection.commit();
            // stmtInitTimeDateRead.close();
        } catch (Exception e) {
            dataAccess_log.error( "Make RDBMS getConnection: " + connectionUrl + " as " + db_userid + " fault:" + sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            return ( (Connection) null );
        }

        dataAccess_log.info( "RDBMS getConnection: `" + connectionUrl + "` as `" + db_userid + "` at `" + dateFormat.format( InitDate ) + "` done" );

      if ( InitDate != null)
        dataAccess_log.info( "RDBMS current_timestamp: LocalDate ="+ InitDate.toString() + " getTime=" + InitDate.getTime() + " mSec., " + dateFormat.format( InitDate )  );
      /*
        try {
                if ( pTarget_Connection != null ) pTarget_Connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return ( (Connection) null );
        }
        */

        return Target_Connection;
    }

    public static String getCurrentTimeString(@NotNull Logger dataAccess_log ) {
        String CurrentTime="00000000000000";
        try {

            ResultSet rs = null;
            stmtCurrentTimeStringRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeStringRead );
            rs = stmtCurrentTimeStringRead.executeQuery();
            while (rs.next()) {
                CurrentTime = rs.getString("InitTime");
            }
            rs.close();
            stmtCurrentTimeStringRead.close();
            DataAccess.Hermes_Connection.commit();
            dataAccess_log.info( "Hermes CurrentTime: LocalDate ="+ CurrentTime );
        } catch (Exception e) {
            dataAccess_log.error("getCurrentTimeString fault: " +  sStackTrace.strInterruptedException(e));

        }
        return ( CurrentTime );

    }

    public static Long getCurrentTime(@NotNull Logger dataAccess_log ) {
        Timestamp CurrentTime=null;
        try {
            ResultSet rs = null;
            stmtCurrentTimeDateRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeDateRead );
            rs = stmtCurrentTimeDateRead.executeQuery();
            while (rs.next()) {
                CurrentTime = rs.getTimestamp("InitTime");
            }
            rs.close();
            stmtCurrentTimeDateRead.close();
            DataAccess.Hermes_Connection.commit();
            stmtCurrentTimeDateRead = null;
            if ( CurrentTime != null)
                dataAccess_log.info( "Hermes CurrentTime: LocalDate ="+ CurrentTime.toString() + " getTime=" + CurrentTime.getTime()  + " mSec., " + dateFormat.format( CurrentTime )  );
            return CurrentTime.getTime();

        } catch (Exception e) {
            dataAccess_log.error("getCurrentTimeDate fault: "  + sStackTrace.strInterruptedException(e)); // + e.getMessage() );
            if ( stmtCurrentTimeDateRead !=null )
                try {stmtCurrentTimeDateRead.close();
                    stmtCurrentTimeDateRead = null;
                }  catch (SQLException SQLe) {
                    dataAccess_log.error("stmtCurrentTimeDateRead.close() 4 executeQuery(" + SQLCurrentTimeDateRead + " fault: " + sStackTrace.strInterruptedException(e));
                }
            try { DataAccess.Hermes_Connection.rollback(); }  catch (SQLException SQLe) {
                dataAccess_log.error("Connection.rollback() 4 executeQuery(" + SQLCurrentTimeDateRead + " fault: " + sStackTrace.strInterruptedException(e));
            }
            return null;
        }
    }

    public static Integer moveERROUT2RESOUT(@NotNull String pSQL_function, @NotNull Logger dataAccess_log ) {
        CallableStatement callableStatement =null;
        String from_callableStatement;

        // # Необходимо обеспечить периодический вызов pl-sql функции, например, для очистки Message_Queue
        try {
             callableStatement = DataAccess.Hermes_Connection.prepareCall (pSQL_function);

        } catch (Exception e) {
            dataAccess_log.error("prepareCall(" + pSQL_function + " fault: " + e.getMessage() ); // + sStackTrace.strInterruptedException(e));
            return null;
        }
        try {
            dataAccess_log.info("try executeCall(" + pSQL_function);
        // register OUT parameter
            callableStatement.registerOutParameter(1, Types.VARCHAR);

        // Step 2.C: Executing CallableStatement
            try {
            callableStatement.execute();
            } catch (SQLException e) {
                dataAccess_log.error(", SQLException callableStatement.execute(" + pSQL_function + " ):=" + e.toString());
                callableStatement.close();
                callableStatement =null;
                try { DataAccess.Hermes_Connection.rollback(); }  catch (SQLException SQLe) {
                    dataAccess_log.error("Connection.rollback() 4 executeCall(" + pSQL_function + " fault: " + sStackTrace.strInterruptedException(e));
                }
                return -3;
            }

        // get count and print in console
            from_callableStatement = callableStatement.getString(1);
            dataAccess_log.info( pSQL_function + " = " + from_callableStatement);
            callableStatement.close();
            DataAccess.Hermes_Connection.commit();

        } catch (Exception e) {
            dataAccess_log.error("executeCall(" + pSQL_function + " fault: " + sStackTrace.strInterruptedException(e));
            if ( callableStatement !=null )
                try {callableStatement.close();} catch (SQLException SQLe) {
                    dataAccess_log.error("callableStatement.close() 4 executeCall(" + pSQL_function + " fault: " + sStackTrace.strInterruptedException(e));
                }
            try { DataAccess.Hermes_Connection.rollback(); }  catch (SQLException SQLe) {
                dataAccess_log.error("Connection.rollback() 4 executeCall(" + pSQL_function + " fault: " + sStackTrace.strInterruptedException(e));
            }
            return -4;
        }

        return 0;
    }

}


