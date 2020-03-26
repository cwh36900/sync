package com.piesat.sod.sync.utils;

import java.math.BigDecimal;

/**
 * @author cwh
 * @date 2020年 03月24日 14:40:56
 */
public class CronUtils {
    public static String getCron(BigDecimal timeInterval){
        String cron = "0 %s %s * * ?";
        BigDecimal[] bigDecimals = timeInterval.divideAndRemainder(BigDecimal.valueOf(1));
        int H = bigDecimals[0].intValue();
        int M = bigDecimals[1].multiply(BigDecimal.valueOf(60)).intValue();
        String HH = "*";
        String MM = "*";
        if (H != 0){
            HH = "0/"+H;
        }

        if (M != 0){
            if (H != 0){
                MM = Integer.toString(M);
            }else {
                MM = "0/"+M;
            }

        }
        cron = String.format(cron,MM,HH);
        return cron;
    }

    public static void main(String[] args) {
        BigDecimal bd = new BigDecimal("1.5");
        System.out.println(CronUtils.getCron(bd));
    }
}
