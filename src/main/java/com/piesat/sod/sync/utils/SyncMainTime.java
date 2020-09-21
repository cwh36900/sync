package com.piesat.sod.sync.utils;

import com.gbase.jdbc.JDBC4PreparedStatement;
import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;

import javax.xml.crypto.Data;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author cwh
 * @date 2020年 04月27日 19:00:32
 */
public class SyncMainTime {

    static Connection connection = null;

    static DruidUtils bfdbDruid = null;
    static Connection bfdbCon = null;

    static DruidUtils stdbDruid = null;
    static Connection stdbCon = null;

    static DruidUtils hadbDruid  = null;
    static Connection hadbCon = null;


    static {
        DatabaseEntity bfdb = new DatabaseEntity();
        bfdb.setDatabaseId("bfdb");
        bfdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        bfdb.setDatabaseUrl("jdbc:xugu://10.20.63.192:5138/BABJ_BFDB?ips=10.20.63.193,10.20.63.194&char_set=utf8&recv_mode=0");
        bfdb.setDatabaseUser("usr_manager");
        bfdb.setDatabasePassword("manager_123");
        DruidBuilder.druidConfigMap.put("bfdb", bfdb);
        DatabaseEntity stdb = new DatabaseEntity();
        stdb.setDatabaseId("stdb");
        stdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        stdb.setDatabaseUrl("jdbc:xugu://10.20.63.196:5138/BABJ_STDB?ips=10.20.63.197,10.20.63.198,10.20.63.210,10.20.63.212,10.20.63.214&char_set=utf8&recv_mode=2");
        stdb.setDatabaseUser("usr_manager");
        stdb.setDatabasePassword("manager_123");
        DruidBuilder.druidConfigMap.put("stdb", stdb);
        DatabaseEntity hadb = new DatabaseEntity();
        hadb.setDatabaseId("hadb");
        hadb.setDriverClass("com.gbase.jdbc.Driver");
        hadb.setDatabaseUrl("jdbc:gbase://10.20.64.29:5258/usr_sod?useOldAliasMetadataBehavior=true&rewriteBatchedStatements=true&connectTimeout=0&hostList=10.20.64.29,10.20.64.30,10.20.64.31&failoverEnable=true");
        hadb.setDatabaseUser("usr_manager");
        hadb.setDatabasePassword("Manager_123");
        DruidBuilder.druidConfigMap.put("hadb", hadb);

        DatabaseEntity smdb = new DatabaseEntity();
        smdb.setDatabaseId("smdb");
        smdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        smdb.setDatabaseUrl("jdbc:xugu://10.20.64.167:5138/BABJ_SMDB?ips=10.20.64.168,10.20.64.169&recv_mode=0&char_set=utf8");
        smdb.setDatabaseUser("USR_SOD");
        smdb.setDatabasePassword("Pnmic_qwe123");
        DruidBuilder.druidConfigMap.put("smdb", smdb);


        bfdbDruid = DruidBuilder.Builder("bfdb");
        try {
            bfdbCon = bfdbDruid.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        stdbDruid = DruidBuilder.Builder("stdb");
        try {
            stdbCon = stdbDruid.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        hadbDruid = DruidBuilder.Builder("hadb");
        try {
            hadbCon = hadbDruid.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static Statement s = null;
    static Statement s1 = null;
    static Statement s2 = null;
    static Statement s3 = null;

    static ResultSet rs = null;
    static ResultSet rs1 = null;
    static ResultSet rs2 = null;
    static ResultSet rs3 = null;

    static String sql = "SELECT %s,D_IYMDHM FROM %s.%s where D_DATETIME >= '2020-08-21 08:00:00.0' and D_DATETIME < '2020-08-21 10:00:00.0'  limit 100";//and D_IYMDHM = D_UPDATE_TIME
    static String sql1 = "SELECT D_IYMDHM FROM %s.%s %s";

    public static void main(String[] args) {


        List<String> lll = new ArrayList<>();
//        lll.add("SURF_WEA_CHN_RSD_TAB");
//        lll.add("SURF_WEA_CHN_TRAFW_PRE_TAB");
//        lll.add("UPAR_WEA_CHN_MUL_NSEC_K_TAB");
//        lll.add("SURF_WEA_CHN_MUL_MIN_TAB");
//        lll.add("SURF_WEA_GLB_MUL_HOR_TAB");
//        lll.add("SURF_WEA_CHN_MUL_HOR_TAB");
//        lll.add("AGME_CROP01_CHN_TAB");
//        lll.add("SEVP_WEFC_RFFC_K_TAB");
//        lll.add("SURF_WEA_CHN_MUL_TEN_TAB");
//        lll.add("UPAR_WEA_GLB_MUL_TEN_TAB");
//        lll.add("OCEN_TSC_GLB_MUL_TAB");
//        lll.add("OCEN_SHB_GLB_MUL_TAB");
        lll.add("UPAR_WPF_CHN_TAB");

        String schema = "USR_SOD";



        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {


            StringBuffer sb = new StringBuffer();
            DruidUtils smdbDruid = DruidBuilder.Builder("smdb");
            connection = smdbDruid.getConnection();
            s = connection.createStatement();
            String smdbSql = "select distinct b.ID,b.SOURCE_TABLE_NAME,c.UNIQUE_KEYS from   USR_SOD.T_SOD_JOB_SYNCMAPPING_INFO b " +
                    "inner join USR_SOD.T_SOD_JOB_SYNCCONFIG_INFO c on b.TARGET_TABLE_ID = concat(c.id,'')";

            String tSql = "select SOURCE_TABLE,SYNC_TYPE from T_SOD_JOB_SYNCTASK_INFO where exec_ip <> '127.0.0.1'";
            ResultSet resultSet = s.executeQuery(tSql);
            Map<String,String> types = new HashMap<>();
            while (resultSet.next()) {
                String source_table = resultSet.getString("SOURCE_TABLE");
                String sync_type = resultSet.getString("SYNC_TYPE");
                String[] split = source_table.split(",");
                for (String t:split) {
                    types.put(t,sync_type);
                }
            }
            resultSet.close();
            rs = s.executeQuery(smdbSql);
            while (rs.next()) {
                String source_table_name = rs.getString("SOURCE_TABLE_NAME");
                String unique_keys = rs.getString("UNIQUE_KEYS");
                String source_table = rs.getString("ID");
                if (unique_keys.indexOf("D_DATETIME") == -1) {
                    unique_keys += ",D_DATETIME";
                }
                String[] keys = unique_keys.split(",");

                if (!lll.contains(source_table_name)) {
                    continue;
                }

                System.out.println(source_table_name + "    " + unique_keys);
                List<Long> s = new ArrayList<>();
                List<Long> h = new ArrayList<>();

                if ("3".equals(types.get(source_table))) {
                    lll.remove(source_table_name);
                s1 = bfdbCon.createStatement();
                String bfdbSql = String.format(sql, unique_keys, schema, source_table_name);
                try {
                    rs1 = s1.executeQuery(bfdbSql);
                } catch (Exception e) {
                    bfdbDruid.close(null, s1, rs1);
                    System.err.println(bfdbSql);
                    e.printStackTrace();
                    continue;
                }
                while (rs1.next()) {
                    Timestamp d_iymdhm = rs1.getTimestamp("D_IYMDHM");
                    String where = " where ";
                    for (int i = 0; i < keys.length; i++) {
                        String key = keys[i];
                        String val = rs1.getString(key);
                        where += key + " = '" + val + "' ";
                        if (i < keys.length - 1) {
                            where += " and ";
                        }
                    }

                    s2 = stdbCon.createStatement();
                    String stdbSql = String.format(sql1, schema, source_table_name, where);
                    System.out.println(stdbSql);
                    rs2 = s2.executeQuery(stdbSql);
                    while (rs2.next()) {
                        Timestamp stdbTime = rs2.getTimestamp("D_IYMDHM");
                        long limitTime = stdbTime.getTime() - d_iymdhm.getTime();
                        s.add(limitTime);
//                        System.out.println(source_table_name+" 缓冲库："+d_iymdhm+" 服务库："+stdbTime+"  "+limitTime/1000);
                    }
                    stdbDruid.close(null, s2, rs2);

                    s3 = hadbCon.createStatement();
                    String hadbSql = String.format(sql1, schema, source_table_name, where);
                    System.out.println(hadbSql);
                    rs3 = s3.executeQuery(hadbSql);
                    while (rs3.next()) {
                        Timestamp hadbTime = rs3.getTimestamp("D_IYMDHM");
                        long limitTime = hadbTime.getTime() - d_iymdhm.getTime();
                        h.add(limitTime);
//                        System.out.println(source_table_name+" 缓冲库："+d_iymdhm+" 分析库："+hadbTime+"  "+limitTime/1000);
                    }
                    hadbDruid.close(null, s3, rs3);


                }
                bfdbDruid.close(null, s1, rs1);

                    LongSummaryStatistics intSummaryStatistics = s.stream().mapToLong((x) -> x).summaryStatistics();
                    double average = intSummaryStatistics.getAverage();
                    long max = intSummaryStatistics.getMax();
                    long min = intSummaryStatistics.getMin();
                    sb.append(source_table_name).append(" 服务库 平均").append(average).append(" 最大").append(max).append(" 最小").append(min).append("\r\n");
//                System.out.println(source_table_name+" "+average);
                    LongSummaryStatistics intSummaryStatistics1 = h.stream().mapToLong((x) -> x).summaryStatistics();
                    double average1 = intSummaryStatistics1.getAverage();
                    long max1 = intSummaryStatistics1.getMax();
                    long min1 = intSummaryStatistics1.getMin();
                    sb.append(source_table_name).append(" 分析库 平均").append(average1).append(" 最大").append(max1).append(" 最小").append(min1).append("\r\n");
//                System.out.println(source_table_name+" "+average1);
            }else if("1".equals(types.get(source_table))){
                    lll.remove(source_table_name);
                    s2 = stdbCon.createStatement();
                    String bfdbSql = String.format(sql, unique_keys, schema, source_table_name);
                    try {
                        rs2 = s2.executeQuery(bfdbSql);
                    } catch (Exception e) {
                        stdbDruid.close(null, s2, rs2);
                        System.err.println(bfdbSql);
                        e.printStackTrace();
                        continue;
                    }
                    while (rs2.next()) {
                        Timestamp d_iymdhm = rs2.getTimestamp("D_IYMDHM");
                        String where = " where ";
                        for (int i = 0; i < keys.length; i++) {
                            String key = keys[i];
                            String val = rs2.getString(key);
                            where += key + " = '" + val + "' ";
                            if (i < keys.length - 1) {
                                where += " and ";
                            }
                        }


                        s3 = hadbCon.createStatement();
                        String hadbSql = String.format(sql1, schema, source_table_name, where);
//                    System.out.println(hadbSql);
                        rs3 = s3.executeQuery(hadbSql);
                        while (rs3.next()) {
                            Timestamp hadbTime = rs3.getTimestamp("D_IYMDHM");
                            long limitTime = hadbTime.getTime() - d_iymdhm.getTime();
                            h.add(limitTime);
//                        System.out.println(source_table_name+" 缓冲库："+d_iymdhm+" 分析库："+hadbTime+"  "+limitTime);
                        }
                        hadbDruid.close(null, s3, rs3);

                    }
                    stdbDruid.close(null, s2, rs2);

                    LongSummaryStatistics intSummaryStatistics1 = h.stream().mapToLong((x) -> x).summaryStatistics();
                    double average1 = intSummaryStatistics1.getAverage();
                    long max = intSummaryStatistics1.getMax();
                    long min = intSummaryStatistics1.getMin();
                    sb.append(source_table_name).append(" 分析库 平均").append(average1).append(" 最大").append(max).append(" 最小").append(min).append("\r\n");
//                System.out.println(source_table_name+" "+average1);
                }

            }

            smdbDruid.close(connection, s, rs);

            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter("D:/111min.txt");//创建文本文件
                fileWriter.write(sb.toString());//写入 \r\n换行
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            bfdbCon.close();
            stdbCon.close();
            hadbCon.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
