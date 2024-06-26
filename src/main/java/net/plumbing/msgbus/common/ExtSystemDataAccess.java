package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.plumbing.msgbus.ServletApplication;
import org.springframework.boot.jdbc.metadata.HikariDataSourcePoolMetadata;
import org.springframework.context.annotation.Bean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

public class ExtSystemDataAccess {
    public static HikariDataSourcePoolMetadata DataSourcePoolMetadata = null;
    @Bean (destroyMethod = "close")
    public static  HikariDataSource HiDataSource(String JdbcUrl, String Username, String Password, String extSysDataSourceClassName ) throws java.sql.SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        String connectionUrl ;
        if ( JdbcUrl==null) {
            connectionUrl = "jdbc:oracle:thin:@//10.242.36.8:1521/hermes12"; // Test-Capsul !!!
            //connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
        }
        else {
            //connectionUrl = "jdbc:oracle:thin:@"+dst_point;
            //connectionUrl = "jdbc:postgresql:"+dst_point;
            connectionUrl = JdbcUrl;
        }
        String ClassforName;
        if ( connectionUrl.contains("oracle") )
            ClassforName = "oracle.jdbc.driver.OracleDriver";
        else {
            if ( connectionUrl.contains("postgresql") )
            ClassforName = "org.postgresql.Driver";
            else
                if (extSysDataSourceClassName !=null)
                    ClassforName = extSysDataSourceClassName;
                else {
                    ServletApplication.AppThead_log.error("ExtSystemDataAccess() for " + connectionUrl + "fault, extSysDataSourceClassName is null" );
                    return null;
                }
        }

        // when  connectionUrl contains  'PuPoVozer_DevReceiver' - then  Set_config('md.surname.crypt', 'true', false);

//        hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//        hikariConfig.setJdbcUrl( "jdbc:oracle:thin:@"+ JdbcUrl); //("jdbc:oracle:thin:@//10.242.36.8:1521/hermes12");
        ServletApplication.AppThead_log.info( "ExtSystemDataAccess: Try make hikariConfig: JdbcUrl `" + connectionUrl + "` as " + Username + " ["+ Password + "] , Class.forName:" + ClassforName);
        hikariConfig.setDriverClassName(ClassforName);
        hikariConfig.setJdbcUrl(  connectionUrl ); //("jdbc:oracle:thin:@//10.242.36.8:1521/hermes12");

        hikariConfig.setUsername( Username ); //("ARTX_PROJ");
        if ((Password !=null ) && (! Password.isEmpty()))
        hikariConfig.setPassword( Password ); // ("rIYmcN38St5P");

        hikariConfig.setLeakDetectionThreshold(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setConnectionTimeout(TimeUnit.SECONDS.toMillis(30));
        hikariConfig.setValidationTimeout(TimeUnit.MINUTES.toMillis(1));
        hikariConfig.setIdleTimeout(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setMaxLifetime(TimeUnit.MINUTES.toMillis(10));

        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(10);

        hikariConfig.setPoolName("ExtSystemCP");
        if (( connectionUrl.contains("oracle") ) || ( connectionUrl.contains("postgresql") )) {
            if (connectionUrl.indexOf("oracle") > 0)
                hikariConfig.setConnectionTestQuery("SELECT 1 from dual");
            else hikariConfig.setConnectionTestQuery("SELECT 1 ");

            hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
            hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "500");
            hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "4096");
            hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
            hikariConfig.addDataSourceProperty("dataSource.autoCommit", "false");
        } //    dataSource.autoCommit" и для Престо Unrecognized connection property 'dataSource.autoCommit

        ServletApplication.AppThead_log.info( "ExtSystemDataAccess: try make DataSourcePool: " + connectionUrl + " as " + Username + " , Class.forName:" + ClassforName);
        HikariDataSource dataSource;
        try {
            dataSource = new HikariDataSource(hikariConfig);
            //HikariPool hikariPool = new HikariPool(hikariConfig);
            DataSourcePoolMetadata = new HikariDataSourcePoolMetadata(dataSource);
        }
        catch (Exception e)
        { ServletApplication.AppThead_log.error( "new HikariDataSource() fault" + e.getMessage());
            return null;
        }
        ServletApplication.AppThead_log.info( "DataSourcePool ( at start ): getMax: " + DataSourcePoolMetadata.getMax()
                + ", getIdle: " + DataSourcePoolMetadata.getIdle()
                + ", getActive: " + DataSourcePoolMetadata.getActive()
                + ", getMax: " + DataSourcePoolMetadata.getMax()
                + ", getMin: " + DataSourcePoolMetadata.getMin()
        );
        ServletApplication.AppThead_log.info(
                "ConnectionTestQuery: " + dataSource.getConnectionTestQuery()
                        + ", IdleTimeout: " + dataSource.getIdleTimeout()
                        + ", LeakDetectionThreshold: " + dataSource.getLeakDetectionThreshold()
        );

        Connection tryConn;
        try {

             tryConn = dataSource.getConnection();
        }
        catch (java.sql.SQLException e)
        { ServletApplication.AppThead_log.error( "dataSource.getConnection() fault" + e.getMessage());
          return null;
        }
        String connectionTestQuery = "SELECT 1 as test";
        try {
            if ( connectionUrl.indexOf("oracle") > 0 )
                connectionTestQuery = "SELECT 1 from dual";

            PreparedStatement prepareStatement = tryConn.prepareStatement( connectionTestQuery );
            prepareStatement.executeQuery();
            prepareStatement.close();
            ServletApplication.AppThead_log.info( "DataSourcePool ( at " + connectionTestQuery + " ): getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getIdle: " + DataSourcePoolMetadata.getIdle()
                    + ", getActive: " + DataSourcePoolMetadata.getActive()
                    + ", getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getMin: " + DataSourcePoolMetadata.getMin()
                    + ", getUsage: " + DataSourcePoolMetadata.getUsage()
            );
            if ( connectionUrl.indexOf("postgresql") > 0 ) {
                String set_config_Query;
                if ( connectionUrl.indexOf("PuPoVozer_DevReceiver") > 0 ) {
                    set_config_Query = "select set_config('md.surname.crypt', 'true', false)";
                }
                else {
                    set_config_Query = "select set_config('md.surname.crypt', 'false', false)";
                }

                try {
                    PreparedStatement set_config_Statement = tryConn.prepareStatement( set_config_Query );
                    set_config_Statement.executeQuery();
                    set_config_Statement.close();
                    ServletApplication.AppThead_log.info( "DataSourcePool ( at prepareStatement ): set_config: " + set_config_Query
                    );
                    ServletApplication.AppThead_log.info("Try setup Connection: `set SESSION time zone 3; set enable_bitmapscan to off;`");
                    PreparedStatement stmt_SetTimeZone = tryConn.prepareStatement("set SESSION time zone 3; set enable_bitmapscan to off;");//.nativeSQL( "set SESSION time zone 3" );
                    stmt_SetTimeZone.execute();
                    stmt_SetTimeZone.close();
                }
                catch (java.sql.SQLException e)
                { ServletApplication.AppThead_log.error( "dataSource set_config fault `" + set_config_Query + "` :" +  e.getMessage());

                }
            }

            tryConn.close();
            ServletApplication.AppThead_log.info( "getJdbcUrl: "+ hikariConfig.getJdbcUrl());
        }
        catch (java.sql.SQLException e)
        { ServletApplication.AppThead_log.error( "dataSource connectionTestQuery `"+ connectionTestQuery + "` fault `" + connectionTestQuery + "` :" +  e.getMessage() + " \n" + sStackTrace.strInterruptedException(e));
            try { tryConn.close();
                }
            catch (java.sql.SQLException closeE)
            { ServletApplication.AppThead_log.error( "dataSource connectionTestQuery close() fault " +  closeE.getMessage()); }
            throw e;
            //return null;
        }

        return dataSource;
    }
    /* */

}
