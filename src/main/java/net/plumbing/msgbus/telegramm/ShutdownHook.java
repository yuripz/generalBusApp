package net.plumbing.msgbus.telegramm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static  net.plumbing.msgbus.ServletApplication.propJDBC;
import static  net.plumbing.msgbus.ServletApplication.ApplicationName;
//@Configuration
//@ComponentScan(basePackages = "ru.hermes.msgbus.*")
@Component

public class ShutdownHook {
    private static final Logger ShutdownHook_log = LoggerFactory.getLogger(ShutdownHook.class);
    @PreDestroy
//    @Bean(name = "onExit")
    public void onExit() {
        ShutdownHook_log.info("###STOPing###");
        String local_propJDBC;
        if ( propJDBC == null)  local_propJDBC = "jdbc UNKNOWN ! ";
        else local_propJDBC = propJDBC;
        try {
            NotifyByChannel.Telegram_sendMessage( "Stop " + ApplicationName + " on " + InetAddress.getLocalHost().getHostName()+ " (ip " +InetAddress.getLocalHost().getHostAddress() + ", db `" + local_propJDBC + "` ), *exit!*", ShutdownHook_log );
            ShutdownHook_log.warn("Как бы типа => Stop "  + ApplicationName + " on " + InetAddress.getLocalHost().getHostName()+ " (ip " +InetAddress.getLocalHost().getHostAddress() + ", db `" + local_propJDBC + "` ), *exit!*" );
            // Thread.sleep(1 * 1000); InterruptedException |
        } catch ( UnknownHostException e) {
            ShutdownHook_log.error(" хрякнулось InetAddress.getLocalHost().getHostAddress()", e);;
        }
        ShutdownHook_log.info("###STOP FROM THE LIFECYCLE###");

    }
}
