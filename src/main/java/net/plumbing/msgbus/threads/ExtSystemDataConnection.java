package net.plumbing.msgbus.threads;

import com.zaxxer.hikari.HikariDataSource;
import net.plumbing.msgbus.ServletApplication;
import net.plumbing.msgbus.common.ApplicationProperties;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExtSystemDataConnection {
    public  Connection  ExtSystem_Connection=null;

    public  ExtSystemDataConnection( long Queue_Id, Logger dataAccess_log) {
        Connection Target_Connection;

        String rdbmsVendor;
        if ( ApplicationProperties.extSystemDataSource == null ) {
            dataAccess_log.error("[{}] ExtSystem getConnection() fault: ApplicationProperties.extSystemDataSource is null", Queue_Id);
            return ;
        }
        HikariDataSource dataSource= ApplicationProperties.extSystemDataSource;
        String connectionUrl = dataSource.getJdbcUrl();
        //this.dbSchema = HrmsSchema;
        if (connectionUrl.contains("oracle") ) {
            rdbmsVendor = "oracle";
        } else {
            if ( connectionUrl.contains("postgresql") )
            rdbmsVendor = "postgresql";
            else
                rdbmsVendor = "presto";
        }
        dataAccess_log.info("[{}] Try(thead) ExtSystem getConnection: {} as {} rdbmsVendor={}", Queue_Id, connectionUrl, ApplicationProperties.ExtSysDbLogin, rdbmsVendor);


        try {
            Target_Connection = dataSource.getConnection();
            Target_Connection.setAutoCommit(false);
        } catch (SQLException e) {
            dataAccess_log.error("[{}] ExtSystem getConnection() fault: {}", Queue_Id, e.getMessage());
            System.err.println( "["+ Queue_Id + "] ExtSystem getConnection() Exception" );
            e.printStackTrace();
            return ;
        }
        // dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );


        if ( rdbmsVendor.equals("postgresql")) {
            try {
                //.nativeSQL( "set SESSION time zone 3; set enable_bitmapscan to off; set max_parallel_workers_per_gather = 0" );
                PreparedStatement stmt_SetSetupConnection = Target_Connection.prepareStatement(ApplicationProperties.ExtSysPgSetupConnection);
                stmt_SetSetupConnection.execute();
                stmt_SetSetupConnection.close();
                String set_config_Query;

                    String current_setting_Set_Config;
                    if ( connectionUrl.indexOf("PuPoVozer_DevReceiver") > 0 ) {
                        set_config_Query = "select set_config('md.surname.crypt', 'true', false)";
                    }
                    else {
                        set_config_Query = "select set_config('md.surname.crypt', 'false', false)";
                    }

                    try {
                        PreparedStatement set_config_Statement = Target_Connection.prepareStatement( set_config_Query );
                        ResultSet resultSet;
                        resultSet = set_config_Statement.executeQuery();
                        ServletApplication.AppThead_log.info("Target_Connection ( at prepareStatement ): set_config: {}", set_config_Query);
                        if ( resultSet != null ) {
                            if (resultSet.next() ) {
                                current_setting_Set_Config = resultSet.getString(1);
                                ServletApplication.AppThead_log.info("Target_Connection ( at prepareStatement ): set_config: {}", current_setting_Set_Config);
                            }
                            else {
                                ServletApplication.AppThead_log.error("Target_Connection set_config_Statement.executeQuery() fault `{}` : resultSet is empty ", set_config_Query);
                            }
                            resultSet.close();
                        }
                        else {
                            ServletApplication.AppThead_log.error("Target_Connection set_config_Statement.executeQuery() fault `{}` : resultSet == null ", set_config_Query);
                        }
                        set_config_Statement.close();


                    }
                    catch (java.sql.SQLException e)
                    {
                        ServletApplication.AppThead_log.error("Target_Connection set_config fault `{}` :{}", set_config_Query, e.getMessage());

                    }

                dataAccess_log.info("[{}] ExtSystem `set SESSION time zone 3` done ", Queue_Id);
            } catch (SQLException e) {

                dataAccess_log.error("[{}] ExtSystem `set SESSION time zone 3` fault: {}", Queue_Id, e.getMessage());
                System.err.println( "["+ Queue_Id + "] ExtSystem `set SESSION time zone 3` Exception" );
                e.printStackTrace();
                try { Target_Connection.close(); //.close(); ??
                } catch (SQLException SQLe) {
                    dataAccess_log.error("[{}] `ExtSystem Connection.close()` fault: {}", Queue_Id, e.getMessage());
                }
                return ;
            }
        }
        ExtSystem_Connection= Target_Connection;
        return  ;
    }
}
