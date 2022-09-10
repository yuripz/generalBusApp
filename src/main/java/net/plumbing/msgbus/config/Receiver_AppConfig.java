package net.plumbing.msgbus.config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor; //.ThreadPoolTaskExecutor;

@Configuration
@ComponentScan(basePackages = "net.plumbing.msgbus.*")

public class Receiver_AppConfig {
    private static final Logger AppConfig_log = LoggerFactory.getLogger(Receiver_AppConfig.class);

    @Bean(name = "taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {

        ThreadPoolTaskExecutor ThreadPool = new ThreadPoolTaskExecutor();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
        ThreadPool.setCorePoolSize(10 );
        ThreadPool.setMaxPoolSize(11);
        ThreadPool.setWaitForTasksToCompleteOnShutdown(true);
        ThreadPool.setThreadNamePrefix("jms-Reader-");
        AppConfig_log.info( "taskExecutor: getThreadNamePrefix:" + ThreadPool.getThreadNamePrefix() );

        AppConfig_log.info("ThreadPoolTaskExecutor for taskExecutor prepared: CorePoolSize(10), MaxPoolSize(11); ");
        return ThreadPool;
    }
}
