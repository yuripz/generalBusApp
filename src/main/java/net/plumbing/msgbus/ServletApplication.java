package net.plumbing.msgbus;

//import net.plumbing.msgbus.mq.JMS_MessageDirection_MQConnectionFactory;
import net.plumbing.msgbus.telegramm.NotifyByChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.CommandLineRunner;
import javax.jms.JMSException;

//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.web.servlet.config.annotation.EnableWebMvc;
//import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
//import org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.HikariDataAccess;
import net.plumbing.msgbus.common.ExtSystemDataAccess;
import net.plumbing.msgbus.config.ConnectionProperties;
import net.plumbing.msgbus.config.DBLoggingProperties;
import net.plumbing.msgbus.common.sStackTrace;

import net.plumbing.msgbus.config.Receiver_AppConfig;
import net.plumbing.msgbus.config.TelegramProperties;
import net.plumbing.msgbus.init.InitMessageRepository;
import net.plumbing.msgbus.common.DataAccess;
import net.plumbing.msgbus.model.MessageDirections;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageType;

import net.plumbing.msgbus.mq.ActiveMQService;
import net.plumbing.msgbus.mq.StoreMQpooledConnectionFactory;
import net.plumbing.msgbus.threads.JMSReceiveTask;

import java.net.InetAddress;
import java.sql.SQLException;
// import java.util.Properties;
import java.util.concurrent.TimeUnit;

//import de.codecentric.boot.admin.server.config.EnableAdminServer;
//import de.codecentric.boot.admin.client.registration.ApplicationRegistrator;
//import de.codecentric.boot.admin.client.registration.Application;
//import de.codecentric.boot.admin.client.registration.DefaultApplicationFactory;

@SpringBootApplication
//@SpringBootApplication(exclude = { SecurityAutoConfiguration.class, SecurityFilterAutoConfiguration.class })
// --- ! @EnableAdminServer
//@EnableWebMvc
//// @Configuration

// @EnableAutoConfiguration
public class ServletApplication implements CommandLineRunner {

    public static final Logger AppThead_log = LoggerFactory.getLogger(ServletApplication.class);

    @Autowired
    public ConnectionProperties connectionProperties;
    @Autowired
    public DBLoggingProperties dbLoggingProperties;
    @Autowired
    public TelegramProperties telegramProperties;

    public static final String ApplicationName="*Receiver_BUS* v.5.01.25";
    public static String propJDBC;
    public static String propExtJDBC;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(ServletApplication.class, args);
    }
    @Override
    public void run(String... args) throws Exception {
        int i;
        boolean jmsReceiveTaskEnabled;

        ApplicationContext context = new AnnotationConfigApplicationContext(Receiver_AppConfig.class);
        //Application myApplication = Application.create("SpringApplication").healthUrl("http://localhost:8005/actuator/health").serviceUrl("http://localhost:8005/instances").build();
        // ApplicationRegistrator myApplicationRegistrator = new ApplicationRegistrator();

        AppThead_log.info("Hellow for ServletApplication ");
        NotifyByChannel.Telegram_setChatBotUrl( telegramProperties.getchatBotUrl() , AppThead_log );
         propJDBC = connectionProperties.gethrmsPoint();
        if ( propJDBC == null)  propJDBC = "jdbc UNKNOWN ! ";
        else {
            if ( propJDBC.indexOf("//") < 1  ) ; //propJDBC = "jdbc INVALID! `" + propJDBC + "`";
            else {
                propJDBC = propJDBC.substring(propJDBC.indexOf("//") + 2);
                if ( propJDBC.indexOf("/") < 1  ) propJDBC = "INVALID db in jdbc ! `" + propJDBC + "`";
                else
                    propJDBC = propJDBC.substring(0, propJDBC.indexOf("/"));
            }
        }
        propExtJDBC = connectionProperties.getextsysPoint();
        if ( propExtJDBC == null)  propExtJDBC = "Ext jdbc UNKNOWN ! ";
        else {
            if ( propExtJDBC.indexOf("//") < 1  ) ;
            else {
                propExtJDBC = propExtJDBC.substring(propExtJDBC.indexOf("//") + 2);
                if ( propExtJDBC.indexOf("/") < 1  ) propExtJDBC = "INVALID db in jdbc ! `" + propExtJDBC + "`";
                else
                    propExtJDBC = propExtJDBC.substring(0, propExtJDBC.indexOf("/"));
            }
        }

        String sendedMessage_2_Telegram = "Starting " + ApplicationName + " on " + InetAddress.getLocalHost().getHostName()+
                " (ip `" +InetAddress.getLocalHost().getHostAddress() + "`, db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`, extDb `"
                + propExtJDBC+ "` as `"+ connectionProperties.getextsysDbLogin()  + "` )";
        AppThead_log.warn( "testSring:[" + sendedMessage_2_Telegram + "] 4 sent"  );
        NotifyByChannel.Telegram_sendMessage( sendedMessage_2_Telegram, AppThead_log );
         /*NotifyByChannel.Telegram_sendMessage( "Starting "+ ApplicationName + " on " + InetAddress.getLocalHost().getHostName()+ " (ip `" +InetAddress.getLocalHost().getHostAddress() +
                 "`, db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`, extDb `"
                 + propExtJDBC+ "` as `"+ connectionProperties.getextsysDbLogin()  + "`)", AppThead_log );
         */

        AppThead_log.warn(dbLoggingProperties.toString());
        AppThead_log.warn(connectionProperties.toString());
        String propLongRetryCount = connectionProperties.getlongRetryCount();
        if (propLongRetryCount == null) propLongRetryCount = "12";
        String propShortRetryCount = connectionProperties.getshortRetryCount();
        if (propShortRetryCount == null) propShortRetryCount = "3";

        String propLongRetryInterval = connectionProperties.getlongRetryInterval();
        if (propLongRetryInterval == null) propLongRetryInterval = "600";
        String propShortRetryInterval = connectionProperties.getshortRetryInterval();
        if (propShortRetryInterval == null) propShortRetryInterval = "30";

        int ShortRetryCount = Integer.parseInt(connectionProperties.getshortRetryCount() );
        ApplicationProperties.ShortRetryCount = ShortRetryCount;
        int LongRetryCount = Integer.parseInt( connectionProperties.getlongRetryCount()  );
        ApplicationProperties.LongRetryCount = LongRetryCount;
        int ShortRetryInterval = Integer.parseInt( connectionProperties.getshortRetryInterval() );
        ApplicationProperties.ShortRetryInterval = ShortRetryInterval;
        int LongRetryInterval = Integer.parseInt( connectionProperties.getlongRetryInterval() );
        ApplicationProperties.LongRetryInterval = LongRetryInterval;
        int WaitTimeBetweenScan = Integer.parseInt( connectionProperties.getwaitTimeScan() );
        ApplicationProperties.WaitTimeBetweenScan = WaitTimeBetweenScan;
        int NumMessageInScan = Integer.parseInt( connectionProperties.getnumMessageInScan() );
        int ApiRestWaitTime = Integer.parseInt( connectionProperties.getapiRestWaitTime() );
        ApplicationProperties.ApiRestWaitTime = ApiRestWaitTime;
        ApplicationProperties.HrmsSchema =  connectionProperties.gethrmsDbSchema() ;
        AppThead_log.info("hrmsDbSchema = " + ApplicationProperties.HrmsSchema );
        ApplicationProperties.HrmsPoint =  connectionProperties.gethrmsPoint() ;
        ApplicationProperties.hrmsDbLogin = connectionProperties.gethrmsDbLogin();
        ApplicationProperties.hrmsDbPasswd =  connectionProperties.gethrmsDbPasswd();
        ApplicationProperties.ConnectMsgBus = connectionProperties.getconnectMsgBus();
        ApplicationProperties.ExtSysSchema = connectionProperties.getextsysDbSchema();
        if ( ApplicationProperties.ConnectMsgBus == null) ApplicationProperties.ConnectMsgBus = "tcp://localhost:61216";
        if (connectionProperties.getjmsReceiveTaskEnabled().equalsIgnoreCase("true") )
            jmsReceiveTaskEnabled = true;
        else  jmsReceiveTaskEnabled = false;
        AppThead_log.info("jmsReceiveTaskEnabled = " + jmsReceiveTaskEnabled );

//        int FirstInfoStreamId = 101;
//        if ( connectionProperties.getfirstInfoStreamId() != null) FirstInfoStreamId = Integer.parseInt( connectionProperties.getfirstInfoStreamId() );
        String psqlFunctionRun = connectionProperties.getpsqlFunctionRun();
        ApplicationProperties.pSQLFunctionRun = connectionProperties.getpsqlFunctionRun();
        AppThead_log.info("psqlFunctionRun = " + psqlFunctionRun );
        // DefaultApplicationFactory myApplicationFactory = new DefaultApplicationFactory();

        try {
        ApplicationProperties.dataSource = HikariDataAccess.HiDataSource (connectionProperties.gethrmsPoint(),
                connectionProperties.gethrmsDbLogin(),
                connectionProperties.gethrmsDbPasswd()
        );
            ApplicationProperties.DataSourcePoolMetadata = HikariDataAccess.DataSourcePoolMetadata;
    } catch (Exception e) {
        AppThead_log.error("НЕ удалось подключится к базе данных (`" + connectionProperties.gethrmsPoint() + "` ) транспортных сообщений:" + e.getMessage());
            NotifyByChannel.Telegram_sendMessage( "Do stopping " + ApplicationName + " *DB problem* `" +  e.getMessage() +  "` ip:" + InetAddress.getLocalHost().getHostAddress()+ ", db `" + connectionProperties.gethrmsPoint() + "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *stopping*", AppThead_log );
            System.exit(-19);
    }

        AppThead_log.info("message DataSource = " + ApplicationProperties.dataSource );
        if ( ApplicationProperties.dataSource != null )
        {
            AppThead_log.info("message DataSource = " + ApplicationProperties.dataSource
                    + " JdbcUrl:" + ApplicationProperties.dataSource.getJdbcUrl()
                    + " isRunning:" + ApplicationProperties.dataSource.isRunning()
                    + " 4 dbSchema:" + ApplicationProperties.HrmsSchema);
        } else {
            AppThead_log.error("НЕ удалось подключится к базе данных (`" + connectionProperties.gethrmsPoint() + "` ) транспортных сообщений:" );
            NotifyByChannel.Telegram_sendMessage( "Do stopping " + ApplicationName + " *DB problem* `"  +  "` ip:" + InetAddress.getLocalHost().getHostAddress()+ ", db `" + connectionProperties.gethrmsPoint() + "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *stopping*", AppThead_log );
            System.exit(-19);
        }

        ApplicationProperties.ExtSysPoint = connectionProperties.getextsysPoint();
        ApplicationProperties.ExtSysDbLogin = connectionProperties.getextsysDbLogin();
        ApplicationProperties.ExtSysDbPasswd = connectionProperties.getextsysDbPasswd();

        if ( ApplicationProperties.ExtSysPoint.equalsIgnoreCase("NONE") )
        {
            NotifyByChannel.Telegram_sendMessage("Do starting " + ApplicationName + " ` ip:" + InetAddress.getLocalHost().getHostAddress() + ", without extDB `" + connectionProperties.getextsysPoint() + "` as `" + connectionProperties.getextsysDbLogin() + "`)", AppThead_log);
            ApplicationProperties.extSystemDataSource = null;
        }
        else {
            try {
                ApplicationProperties.extSystemDataSource = ExtSystemDataAccess.HiDataSource(connectionProperties.getextsysPoint(),
                        connectionProperties.getextsysDbLogin(),
                        connectionProperties.getextsysDbPasswd(),
                        connectionProperties.getextSysDataSourceClassName()
                );
                ApplicationProperties.extSystemDataSourcePoolMetadata = ExtSystemDataAccess.DataSourcePoolMetadata;
            } catch (Exception e) {
                AppThead_log.error("НЕ удалось подключится к базе данных внешней системы: (" + " )" + e.getMessage());
                NotifyByChannel.Telegram_sendMessage("Do stopping " + ApplicationName + " *extDB problem* `" + e.getMessage() + "` ip:" + InetAddress.getLocalHost().getHostAddress() + ", db `" + connectionProperties.getextsysPoint() + "` as `" + connectionProperties.getextsysDbLogin() + "`), *stopping*", AppThead_log);
                System.exit(-20);
            }
        }

        AppThead_log.info("extSystem DataSource = " + ApplicationProperties.extSystemDataSource );
        if (! ApplicationProperties.ExtSysPoint.equalsIgnoreCase("NONE")) {
            if (ApplicationProperties.extSystemDataSource != null) {
                AppThead_log.info("extSystem DataSource = " + ApplicationProperties.extSystemDataSource
                        + " JdbcUrl:" + ApplicationProperties.extSystemDataSource.getJdbcUrl()
                        + " isRunning:" + ApplicationProperties.extSystemDataSource.isRunning()
                        + " 4 dbSchema:" + ApplicationProperties.ExtSysSchema
                        + " by driver:" + connectionProperties.getextSysDataSourceClassName());
            } else {
                AppThead_log.error("НЕ удалось подключится к базе данных внешней системы: останавливаем");
                NotifyByChannel.Telegram_sendMessage("Do stopping " + ApplicationName + " *extDB problem*  ip:" + InetAddress.getLocalHost().getHostAddress() +
                        ", db `" + connectionProperties.getextsysPoint() + "` as `" + connectionProperties.getextsysDbLogin() + "`), *stopping*", AppThead_log);
                System.exit(-20);
            }
        }


        ActiveMQService activeMQService= new ActiveMQService();
        try {
        activeMQService.MakeActiveMQConnectionFactory( ApplicationProperties.ConnectMsgBus );
        activeMQService.StartJMSQueueConnection("ServletApplication.java: string no.235");
        //activeMQService.StartJMSQueueConnection("333");
        //activeMQService.StartJMSQueueConnection("4444");
        } catch (JMSException e) {
            AppThead_log.error("НЕ удалось подключится к брокеру сообщений ActiveMQ:" + e.getMessage() + "\n" + sStackTrace.strInterruptedException(e));
            System.err.println("НЕ удалось подключится к брокеру сообщений ActiveMQ:");
            e.printStackTrace();

        }
        // AppThead_log.warn("Подключились к брокеру сообщений ActiveMQ!"  );System.exit(-11);

        // Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z

        DataAccess.make_Hermes_Connection( ApplicationProperties.HrmsSchema, ApplicationProperties.dataSource.getConnection(),
                connectionProperties.gethrmsPoint(),
                connectionProperties.gethrmsDbLogin(),
                connectionProperties.gethrmsDbPasswd(),
                AppThead_log
        );

        // Зачитываем MessageDirection
        InitMessageRepository.SelectMsgDirections(ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval,AppThead_log );

        Integer count = 0;
        /* // -- оставлено так, для отладки и вдруг пригодиться что то делать в приёмнике для каждой из системб например .
        int firstInfoStreamId = Integer.parseInt(connectionProperties.getfirstInfoStreamId() );
        int TotalNumTasks= Integer.parseInt( connectionProperties.gettotalNumTasks() );
        for ( int i = 0; i < TotalNumTasks; i ++ )
            AppThead_log.warn( "Thead [" + (firstInfoStreamId + i) + "] ->" +
                                MessageRepositoryHelper.look4MessageDirectionsCode_4_Num_Thread(firstInfoStreamId + i, AppThead_log ) );

        */
        //   System.exit(-22);
        //AppThead_log.info("keysAllMessageDirections: " + MessageDirections.AllMessageDirections.get(1).getMsgDirection_Desc() );

        Long TotalTimeTasks = Long.parseLong( connectionProperties.gettotalTimeTasks());
        Long intervalReInit = Long.parseLong( connectionProperties.getintervalReInit());

        InitMessageRepository.SelectMsgTypes( AppThead_log );
        AppThead_log.info("Read MsgTypes: " + MessageType.AllMessageType.size() + " done" );

        InitMessageRepository.SelectMsgTemplates(  AppThead_log );
        AppThead_log.info("Read MessageTemplates: " + MessageTemplate.AllMessageTemplate.size() + " done" );


        // 1й проход, получаем количество потоков, которые задействованы в  JMS  систем.
        int TotalNumTasks=0; int TotalNumJMS_System=0;
        int MessageDirections_BrokerId=0;
        if (jmsReceiveTaskEnabled) { // Если признак hermes.jms-receive-task-enabled=tru , то тогда и только тогда реально всё запускается
        for (i = 0; i < MessageDirections.AllMessageDirections.size(); i++) {
            if (MessageDirections.AllMessageDirections.get(i).getType_Connect() == 6) {
                TotalNumTasks += MessageDirections.AllMessageDirections.get(i).getNum_Thread();
                TotalNumJMS_System = TotalNumJMS_System + 1;
                AppThead_log.info(i + " TotalNumTasks: " + TotalNumTasks + " ," + MessageDirections.AllMessageDirections.get(i).getMsgDirection_Desc());
                MessageDirections_BrokerId = i; // MessageDirections.AllMessageDirections.get(i).getWSDL_Name();
            }
        }

        AppThead_log.info("Итого имеем TotalNumTasks: " + TotalNumTasks + " для чтения из JMS, систем, читающих JMS=" + TotalNumJMS_System);
        //     ActiveMQService[] jmsMQService = new ActiveMQService[ TotalNumJMS_System ];

        //  - пока считаем, что  JMS_MessageDirection_MQConnectionFactory один, в дальнейшем их должно быть по числу JMS систем, сейчас берется последний
        /* JMS_MessageDirection_MQConnectionFactory вынесен в поток jmsReceiveTask
        if ( JMS_MessageDirection_MQConnectionFactory.MQconnectionFactory == null )
            try {
                JMS_MessageDirection_MQConnectionFactory.MakeActiveMQConnectionFactory(
                        MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getWSDL_Name(),
                        MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getDb_user(),
                        MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getDb_pswd(),
                        AppThead_log  );
            } catch (JMSException e) {
                AppThead_log.error("НЕ удалось подключится к брокеру сообщений ActiveMQ :" + e.getMessage());
            }
        */
        ThreadPoolTaskExecutor taskExecutor = (ThreadPoolTaskExecutor) context.getBean("taskExecutor");
        JMSReceiveTask[] jmsReceiveTask = new JMSReceiveTask[TotalNumTasks];
        Thread.State JMSReceiveThreadState = Thread.State.NEW;
        Thread[] JMSReceiveThread = new Thread[TotalNumTasks];


        // 2-й проход, инициализируем коннекты для каждой из систем, в которой задействованы в  JMS  систем.

        int CurrentTasksIndex = 0;
        int NumTasksInsystem;
        for (MessageDirections_BrokerId = 0; MessageDirections_BrokerId < MessageDirections.AllMessageDirections.size(); MessageDirections_BrokerId++) {
            if (MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getType_Connect() == 6) {
                NumTasksInsystem = MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getNum_Thread();
                for (i = 0; i < NumTasksInsystem; i++) {
                    // не нужен MessageDirectionsCode =  MessageRepositoryHelper.look4MessageDirectionsCode_4_Num_Thread( theadNum + this.FirstInfoStreamId  , AppThead_log );
                    jmsReceiveTask[CurrentTasksIndex] = new JMSReceiveTask();// (MessageSendTask) context.getBean("MessageSendTask");
                    jmsReceiveTask[CurrentTasksIndex].setJMSPoint(MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getWSDL_Name());
                    jmsReceiveTask[CurrentTasksIndex].setJMSLogin(MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getDb_user());
                    jmsReceiveTask[CurrentTasksIndex].setJMSPasswd(MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getDb_pswd());
                    jmsReceiveTask[CurrentTasksIndex].setJMSQueueName(MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getApp_Server());
                    JMSReceiveThread[CurrentTasksIndex] = taskExecutor.createThread(jmsReceiveTask[CurrentTasksIndex]);
                    //  JMSReceiveThread[ i ].run();
                    //  AppThead_log.info("JMSReceiveThread[" + i + "] run: " + JMSReceiveThread[ i ].getName() + " Id=" + JMSReceiveThread[ i ].getId() + " isAlive=" + JMSReceiveThread[ i ].isAlive());
                    // jmsReceiveTask[ i ].setContext(  context );

                    taskExecutor.execute(JMSReceiveThread[CurrentTasksIndex]);
                    AppThead_log.info("JMSReceiveThread[" + CurrentTasksIndex + "] for Q " + MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId).getApp_Server() + " run: " + JMSReceiveThread[CurrentTasksIndex].getName() + " JMSReceiveThread_Id=" + JMSReceiveThread[CurrentTasksIndex].threadId() + " isAlive=" + JMSReceiveThread[CurrentTasksIndex].isAlive());
                    JMSReceiveThreadState = JMSReceiveThread[CurrentTasksIndex].getState();
                    // AppThead_log.info("JMSReceiveThread[" + i + "] JMSReceiveThreadState: " + JMSReceiveThreadState.toString() ) ;
                    // taskExecutor.execute(jmsReceiveTask[ i ]);
                    CurrentTasksIndex = CurrentTasksIndex + 1;
                }
            }
        }
    }


        // int totalTasks = Integer.parseInt( "1" ); // TotalNumTasks; //Integer.parseInt( "50" ); //
        Long CurrentTime;
        CurrentTime = DataAccess.getCurrentTime( AppThead_log );
        DataAccess.InitDate.setTime( CurrentTime );
        AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );
        try {
            if ( DataAccess.Hermes_Connection != null ) DataAccess.Hermes_Connection.close();
        } catch (SQLException e) {
            e.printStackTrace();

        }

        count = 1;

        String CurrentTimeString;
        Long timeToReInit;

        // Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z
        /* Postgre don't make Monitoring_Connection
        AppendDataAccess.make_Monitoring_Connection( dbLoggingProperties.getdataSourceClassName(),
                dbLoggingProperties.getjdbcUrl(),
                dbLoggingProperties.getmntrDbLogin(),
                dbLoggingProperties.getmntrDbPasswd(),
                AppThead_log
        );
        */

        for (;;) {

            AppThead_log.info("Active Threads : " + count);
            AppThead_log.info( "DataSourcePool getMax: " + ApplicationProperties.DataSourcePoolMetadata.getMax()
                    + ", getIdle: " + ApplicationProperties.DataSourcePoolMetadata.getIdle()
                    + ", getActive: " + ApplicationProperties.DataSourcePoolMetadata.getActive()
                    + ", getMax: " + ApplicationProperties.DataSourcePoolMetadata.getMax()
                    + ", getMin: " + ApplicationProperties.DataSourcePoolMetadata.getMin()
            );
            try {

                // Thread.sleep(25000);
                Thread.sleep(TimeUnit.SECONDS.toMillis(30) );
                DataAccess.Hermes_Connection = null;
                DataAccess.Hermes_Connection = ApplicationProperties.dataSource.getConnection();
                DataAccess.Hermes_Connection.setAutoCommit(false);
                CurrentTime = DataAccess.getCurrentTime( AppThead_log );
                CurrentTimeString = DataAccess.getCurrentTimeString( AppThead_log );
                Runtime runTime = Runtime.getRuntime();
                long freeMemory = runTime.maxMemory() - runTime.totalMemory() + runTime.freeMemory();
                AppThead_log.info(" 'free memory' of a Java process before GC is : " + freeMemory );
                runTime.gc();
                Thread.sleep(TimeUnit.SECONDS.toMillis(5) );
                freeMemory = runTime.maxMemory() - runTime.totalMemory() + runTime.freeMemory();
                AppThead_log.info(" 'free memory' of a Java process after GC is : " + freeMemory );

                timeToReInit = (CurrentTime - DataAccess.InitDate.getTime())/1000;
                AppThead_log.info("CurrentTimeString=" +  CurrentTimeString + " (CurrentTime - DataAccess.InitDate.getTime())/1000: " +timeToReInit.toString() + " intervalReInit=" +intervalReInit.toString() );
                if ( timeToReInit > intervalReInit )
                {
                    AppThead_log.info("CurrentTimeString=" +  CurrentTimeString + " (CurrentTime - DataAccess.InitDate.getTime())/1000: " +timeToReInit.toString() );

                    InitMessageRepository.ReReadMsgTypes(  intervalReInit, AppThead_log );
                    InitMessageRepository.ReReadMsgTemplates( intervalReInit, AppThead_log);
                    DataAccess.InitDate.setTime( CurrentTime );
                    AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );

                    // если указана pl-sql функция, то она будет периодически исполняться
                    if (( psqlFunctionRun != null) && ( !psqlFunctionRun.equalsIgnoreCase("NONE")) )
                        DataAccess.moveERROUT2RESOUT( psqlFunctionRun, AppThead_log );
                }
                DataAccess.Hermes_Connection.close();
                DataAccess.Hermes_Connection = null;

                if ( StoreMQpooledConnectionFactory.MQpooledConnectionFactory == null )
                    try {
                        activeMQService.MakeActiveMQConnectionFactory( ApplicationProperties.ConnectMsgBus );
                        activeMQService.StartJMSQueueConnection("ServletApplication.java: string no.123-362");
                        AppThead_log.warn("Удалось пере-подключится к встренному брокеру сообщений ActiveMQ :" + ApplicationProperties.ConnectMsgBus );
                    } catch (JMSException e) {
                        AppThead_log.error("НЕ удалось подключится встренному  к брокеру сообщений ActiveMQ [" + ApplicationProperties.ConnectMsgBus + "] :" + e.getMessage());
                        System.err.println("НЕ удалось подключится к брокеру сообщений ActiveMQ:");
                        e.printStackTrace();
                    }
                /* JMS_MessageDirection_MQConnectionFactory вынесен в поток jmsReceiveTask
                if ( JMS_MessageDirection_MQConnectionFactory.MQconnectionFactory == null )
                    try {
                        JMS_MessageDirection_MQConnectionFactory.MakeActiveMQConnectionFactory(
                                MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getWSDL_Name(),
                                MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getDb_user(),
                                MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getDb_pswd(),
                                AppThead_log  );
                        AppThead_log.warn("Удалось пере-подключится к брокеру сообщений ActiveMQ :" + MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getWSDL_Name() );
                    } catch (JMSException e) {
                        AppThead_log.error("НЕ удалось пере-подключится к брокеру сообщений ActiveMQ [" + MessageDirections.AllMessageDirections.get(MessageDirections_BrokerId ).getWSDL_Name() + "] :" + e.getMessage());
                    }
                */
                /* JMSReceiveThread[i].getState() не показывает состояние потоко, закомментарено
                if (jmsReceiveTaskEnabled) { // Если признак hermes.jms-receive-task-enabled=tru , то тогда и только тогда реально всё запускается
                    for (i=0; i< TotalNumTasks; i++) {
                        JMSReceiveThreadState = JMSReceiveThread[i].getState();
                        AppThead_log.info("JMSReceiveThread[" + i + "] (`"+ JMSReceiveThread[i].getName() + "`) JMSReceiveThreadState: " + JMSReceiveThreadState.toString());
                    }
                }*/

            } catch (InterruptedException | SQLException e) {
                AppThead_log.error("do taskExecutor.shutdown! " + e.getMessage());
                e.printStackTrace();
                NotifyByChannel.Telegram_sendMessage( "Do "+ ApplicationName + " taskExecutor.Shutdown -`" +  e.getMessage() +  "` :" + InetAddress.getLocalHost().getHostAddress()+ ", db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *exit!*", AppThead_log );
                if (DataAccess.Hermes_Connection != null)
                    try {
                        DataAccess.Hermes_Connection.close();
                    }
                        catch ( SQLException SQLe) {
                            AppThead_log.error(" С базой совсем всё плохо! " + SQLe.getMessage());
                            NotifyByChannel.Telegram_sendMessage( "Stopping " + ApplicationName + " *DBproblem* `" +  SQLe.getMessage() +  "` :" + InetAddress.getLocalHost().getHostAddress()+ ", db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *exit!*", AppThead_log );
                        }

                count = 0; // надо taskExecutor.shutdown();
                break;
            }
            if (count == 0) {
                // taskExecutor.shutdown();
                break;
            }
        }
        ApplicationProperties.dataSource.close();
        NotifyByChannel.Telegram_sendMessage( "Stop "+  ApplicationName + " (`" + InetAddress.getLocalHost().getHostAddress()+ "`, db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *exit!*", AppThead_log );
        System.exit(-22);

    }

/* */

}


