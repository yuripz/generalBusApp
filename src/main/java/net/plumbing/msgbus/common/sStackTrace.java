package net.plumbing.msgbus.common;
import java.io.PrintWriter;
import java.io.StringWriter;

public class sStackTrace {
    public static String strInterruptedException (Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);


        String sStackTrace = sw.toString(); // stack trace as a string
        int len_sStackTrace = sStackTrace.length();
        if (len_sStackTrace > 1536) {
            len_sStackTrace = 1536;
        }
        return sStackTrace.substring(0, len_sStackTrace);

    }
}
