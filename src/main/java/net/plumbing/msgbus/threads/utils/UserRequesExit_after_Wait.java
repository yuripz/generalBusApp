package net.plumbing.msgbus.threads.utils;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

public class UserRequesExit_after_Wait extends Thread {
    private Logger Message_Log;
    public void set_Message_Log_Logger( Logger Message_Log){
        this.Message_Log = Message_Log;
    }

    public void run() {
        //NotifyByChannel.Telegram_sendMessage("Do stopping " + ApplicationName + " *extDB problem* `" + e.getMessage() + "` ip:" + InetAddress.getLocalHost().getHostAddress() + ", db `" + connectionProperties.getextsysPoint() + "` as `" + connectionProperties.getextsysDbLogin() + "`), *stopping*", AppThead_log);
        Thread currentThread = Thread.currentThread();
        if ( Message_Log!=null ) {
            Message_Log.info("UserRequesExit_after_Wait, Thread.sleep {}", TimeUnit.SECONDS.toMillis(5) );
        }
        try {
              sleep(TimeUnit.SECONDS.toMillis(5) );
           } catch (InterruptedException e) {
            if ( Message_Log!=null ) {
                Message_Log.info("UserRequesExit_after_Wait, Thread.sleep {}", TimeUnit.SECONDS.toMillis(5) );
            }
             else e.printStackTrace();
            }
        if ( Message_Log!=null ) {
            Message_Log.info("UserRequesExit_after_Wait: UserRequesExit System.exit( {} )", 33 );
        }
        System.exit(-33);
    }
}
