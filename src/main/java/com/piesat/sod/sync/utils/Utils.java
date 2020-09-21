package com.piesat.sod.sync.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author cwh
 * @date 2020年 03月26日 10:20:41
 */
public class Utils {

    public static String getExceptionToString(Throwable e) {
        if (e == null) {
            return "";
        }
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    public static void main(String[] args) {
        System.out.print(String.format("%-45s", "中国地面自动站运行状态和设备信息（XML格式）历史分析库同步"));
    }
}
