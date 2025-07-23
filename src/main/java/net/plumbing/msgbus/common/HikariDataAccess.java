package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.plumbing.msgbus.ServletApplication;
import org.springframework.boot.jdbc.metadata.HikariDataSourcePoolMetadata;
import org.springframework.context.annotation.Bean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

public class HikariDataAccess {
    public static HikariDataSourcePoolMetadata DataSourcePoolMetadata = null;
    @Bean (destroyMethod = "close")
    public static  HikariDataSource HiDataSource(String JdbcUrl, String Username, String Password ){
        HikariConfig hikariConfig = new HikariConfig();
        String connectionUrl ;
        if ( JdbcUrl==null) {
            connectionUrl = "jdbc:oracle:thin:@//5.6.7.8:1521/hermesXX"; // Test-Capsul !!!
        }
        else {
            //connectionUrl = "jdbc:oracle:thin:@"+dst_point;
            //connectionUrl = "jdbc:postgresql:"+dst_point;
            connectionUrl = JdbcUrl;
        }
        String ClassforName;
        if ( connectionUrl.indexOf("oracle") > 0 ) {
            ClassforName = "oracle.jdbc.driver.OracleDriver";
            XMLchars.MAX_TAG_VALUE_BYTE_SIZE = 3992; // for Oracle, it must be 3992
        }
        else {
            ClassforName = "org.postgresql.Driver";
            XMLchars.MAX_TAG_VALUE_BYTE_SIZE= 32778; //  for PostGreSQL 32778;
        }

//        hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//        hikariConfig.setJdbcUrl( "jdbc:oracle:thin:@"+ JdbcUrl);
        ServletApplication.AppThead_log.info("Try make hikariConfig: {} as {} , Class.forName:{}", connectionUrl, Username, ClassforName);
        hikariConfig.setDriverClassName(ClassforName);
        hikariConfig.setJdbcUrl(  connectionUrl );

        hikariConfig.setUsername( Username );
        hikariConfig.setPassword( Password );
        hikariConfig.setLeakDetectionThreshold(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setConnectionTimeout(TimeUnit.SECONDS.toMillis(30));
        hikariConfig.setValidationTimeout(TimeUnit.MINUTES.toMillis(1));
        hikariConfig.setIdleTimeout(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setMaxLifetime(TimeUnit.MINUTES.toMillis(10));

        hikariConfig.setMaximumPoolSize(100);
        hikariConfig.setMinimumIdle(10);
          if ( connectionUrl.indexOf("oracle") > 0 )
        hikariConfig.setConnectionTestQuery("SELECT 1 from dual");
          else
        hikariConfig.setConnectionTestQuery("SELECT 1 ");
        hikariConfig.setPoolName("MessageCP");

        hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "500");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "4096");
        hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.autoCommit", "false");
        ServletApplication.AppThead_log.info( "Try make DataSourcePool: " + connectionUrl + " as " + Username + " , Class.forName:" + ClassforName);
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        //HikariPool hikariPool = new HikariPool(hikariConfig);
        DataSourcePoolMetadata = new HikariDataSourcePoolMetadata(dataSource);
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

        try {

            Connection tryConn = dataSource.getConnection();
            PreparedStatement prepareStatement;
            if ( connectionUrl.indexOf("oracle") > 0 )
                prepareStatement = tryConn.prepareStatement( "SELECT 1 from dual");
            else
                prepareStatement = tryConn.prepareStatement( "SELECT 1 ");
            prepareStatement.executeQuery();
            prepareStatement.close();
            ServletApplication.AppThead_log.info( "DataSourcePool ( at prepareStatement ): getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getIdle: " + DataSourcePoolMetadata.getIdle()
                    + ", getActive: " + DataSourcePoolMetadata.getActive()
                    + ", getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getMin: " + DataSourcePoolMetadata.getMin()
            );
            tryConn.close();
            ServletApplication.AppThead_log.info( "getJdbcUrl: "+ hikariConfig.getJdbcUrl());
        }
        catch (java.sql.SQLException e)
        { ServletApplication.AppThead_log.error( e.getMessage());}


        return dataSource;
    }
    /* */
}
