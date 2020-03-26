package com.piesat.sod.sync.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author cwh
 * @date 2020年 03月26日 10:20:41
 */
public class Utils {

    public static String getExceptionToString(Throwable e) {
        if (e == null){
            return "";
        }
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

}
