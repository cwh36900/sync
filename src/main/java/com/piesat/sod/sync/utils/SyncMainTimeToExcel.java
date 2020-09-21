package com.piesat.sod.sync.utils;

import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;
import org.apache.poi.xssf.usermodel.XSSFPivotTable;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author cwh
 * @date 2020年 04月27日 19:00:32
 */
public class SyncMainTimeToExcel {

    static Connection connection = null;

    static DruidUtils bfdbDruid = null;
    static Connection bfdbCon = null;

    static DruidUtils stdbDruid = null;
    static Connection stdbCon = null;

    static DruidUtils hadbDruid = null;
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

    static String sql = "SELECT %s FROM %s.%s WHERE %s";
    static String where = "D_DATETIME >= '%s' AND D_DATETIME < '%s'";
    static String beginTime = "2020-09-06 00:00:00";
    static String endTime = "2020-09-08 00:00:00";


    public static void main(String[] args) {
        String path = "D:\\导出\\UPAR_WEA_GLB_MUL_FTM_TAB.xlsx";

        List<String> lll = new ArrayList<>();

        lll.add("UPAR_WEA_GLB_MUL_FTM_TAB");

        String schema = "USR_SOD";

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter s_formatter = DateTimeFormatter.ofPattern("dd日HH点");

        LocalDateTime begin = LocalDateTime.parse(beginTime, formatter);
        LocalDateTime end = LocalDateTime.parse(endTime, formatter);

        List<String> whereList = new ArrayList<>();

        List<String> flags = new ArrayList<>();

        while (begin.isBefore(end)) {
            String t_beginTime = begin.format(formatter);
            String tt_beginTime = begin.format(s_formatter);
            begin = begin.plusHours(1);
            String t_endTime = begin.format(formatter);
            String format = String.format(where, t_beginTime, t_endTime);
            whereList.add(format);
            flags.add(tt_beginTime);
        }


        try {
            List<Map<String, Object>> maps = new ArrayList<>();

            DruidUtils smdbDruid = DruidBuilder.Builder("smdb");
            connection = smdbDruid.getConnection();
            s = connection.createStatement();
            String smdbSql = "select distinct b.ID,b.SOURCE_TABLE_NAME,c.UNIQUE_KEYS from   USR_SOD.T_SOD_JOB_SYNCMAPPING_INFO b " +
                    "inner join USR_SOD.T_SOD_JOB_SYNCCONFIG_INFO c on b.TARGET_TABLE_ID = concat(c.id,'')";

            String tSql = "select SOURCE_TABLE,SYNC_TYPE from T_SOD_JOB_SYNCTASK_INFO where exec_ip <> '127.0.0.1'";
            ResultSet resultSet = s.executeQuery(tSql);
            Map<String, String> types = new HashMap<>();
            while (resultSet.next()) {
                String source_table = resultSet.getString("SOURCE_TABLE");
                String sync_type = resultSet.getString("SYNC_TYPE");
                String[] split = source_table.split(",");
                for (String t : split) {
                    types.put(t, sync_type);
                }
            }
            resultSet.close();
            rs = s.executeQuery(smdbSql);
            int re = 0;
            while (rs.next()) {
                String source_table_name = rs.getString("SOURCE_TABLE_NAME");
                String source_table = rs.getString("ID");
                String unique_keys = rs.getString("UNIQUE_KEYS");
                unique_keys += ",D_IYMDHM";
                String[] split = unique_keys.split(",");
                String collect = Arrays.stream(split).distinct().collect(Collectors.joining(","));

                if (!lll.contains(source_table_name)) {
                    continue;
                }
                List<Long> s = new ArrayList<>();
                List<Long> h = new ArrayList<>();

                if ("3".equals(types.get(source_table))) {
                    lll.remove(source_table_name);
                    for (int i = 0; i < whereList.size(); i++) {
                        Map<String, Object> map = new HashMap<>();
                        List<String> titles = new ArrayList<>();

                        String w = whereList.get(i);
                        w = w + " ORDER BY " + collect;
                        String querySql = String.format(sql, collect, schema, source_table_name, w);
                        System.out.println(querySql);
                        map.put("name", flags.get(i));
                        System.out.println(flags.get(i));
                        s1 = bfdbCon.createStatement();
                        long l1 = System.currentTimeMillis();
                        rs1 = s1.executeQuery(querySql);
                        long l2 = System.currentTimeMillis();
                        System.out.println("BFDB耗时:" + (l2 - l1));
                        List<String> bfdbTitles = getColumnNameList(rs1, "BFDB");
                        titles.addAll(bfdbTitles);
                        List<Map<String, Object>> bfdbMap = getResultSetToMap(rs1, "BFDB");
                        bfdbDruid.close(null, s1, rs1);
                        System.out.println("BFDB " + bfdbMap.size());
                        s2 = stdbCon.createStatement();
                        long l3 = System.currentTimeMillis();
                        rs2 = s2.executeQuery(querySql);
                        long l4 = System.currentTimeMillis();
                        System.out.println("STDB耗时:" + (l4 - l3));
                        List<String> stdbTitles = getColumnNameList(rs2, "STDB");
                        titles.addAll(stdbTitles);
                        List<Map<String, Object>> stdbMap = getResultSetToMap(rs2, "STDB");
                        stdbDruid.close(null, s2, rs2);
                        System.out.println("STDB " + stdbMap.size());
                        s3 = hadbCon.createStatement();
                        long l5 = System.currentTimeMillis();
                        rs3 = s3.executeQuery(querySql);
                        long l6 = System.currentTimeMillis();
                        System.out.println("HADB耗时:" + (l6 - l5));
                        List<String> hadbTitles = getColumnNameList(rs3, "HADB");
                        titles.addAll(hadbTitles);
                        List<Map<String, Object>> hadbMap = getResultSetToMap(rs3, "HADB");
                        hadbDruid.close(null, s3, rs3);
                        System.out.println("HADB " + hadbMap.size());

                        if (bfdbMap.size() != stdbMap.size() || bfdbMap.size() != hadbMap.size()) {
                            System.out.println("数据不一致");
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            if (re < 10) {
                                i--;
                                re++;
                            } else {
                                System.out.println("舍弃：" + w);
                                re = 0;
                            }
                            continue;
                        }
                        titles.add("服务库差值(ms)");
                        titles.add("分析库差值(ms)");
                        map.put("titles", titles);
                        List<Map<String, Object>> dataList = new ArrayList<>();
                        for (int j = 0; j < bfdbMap.size(); j++) {
                            Map<String, Object> dataMap = new HashMap<>();
                            Map<String, Object> bfdbObjectMap = bfdbMap.get(j);
                            Timestamp b_iymdhm = (Timestamp) bfdbObjectMap.get("BFDB_D_IYMDHM");
                            long time1 = b_iymdhm.getTime();
                            Map<String, Object> stdbObjectMap = stdbMap.get(j);
                            Timestamp s_iymdhm = (Timestamp) stdbObjectMap.get("STDB_D_IYMDHM");
                            long time2 = s_iymdhm.getTime();
                            s.add(time2 - time1);
                            Map<String, Object> hadbObjectMap = hadbMap.get(j);
                            Timestamp h_iymdhm = (Timestamp) hadbObjectMap.get("HADB_D_IYMDHM");
                            long time3 = h_iymdhm.getTime();
                            h.add(time3 - time1);
                            dataMap.putAll(bfdbObjectMap);
                            dataMap.putAll(stdbObjectMap);
                            dataMap.putAll(hadbObjectMap);
                            Map<String, Object> sm = new HashMap<>();
                            sm.put("服务库差值(ms)", (time2 - time1));
                            Map<String, Object> hm = new HashMap<>();
                            hm.put("分析库差值(ms)", (time3 - time1));
                            dataMap.putAll(sm);
                            dataMap.putAll(hm);
                            dataList.add(dataMap);
                        }
                        maps.add(map);
                        map.put("values", dataList);

                    }
                    LongSummaryStatistics intSummaryStatistics = s.stream().mapToLong((x) -> x).summaryStatistics();
                    double average = intSummaryStatistics.getAverage();
                    long max = intSummaryStatistics.getMax();
                    LongSummaryStatistics intSummaryStatistics1 = h.stream().mapToLong((x) -> x).summaryStatistics();
                    double average1 = intSummaryStatistics1.getAverage();
                    long max1 = intSummaryStatistics1.getMax();
                    List<String> titles = new ArrayList<>();
                    titles.add("数据库");
                    titles.add("平均值");
                    titles.add("最大值");
                    Map<String, Object> map = new HashMap<>();
                    map.put("数据库", "服务库");
                    map.put("平均值", average);
                    map.put("最大值", max);
                    Map<String, Object> map1 = new HashMap<>();
                    map1.put("数据库", "分析库");
                    map1.put("平均值", average1);
                    map1.put("最大值", max1);
                    List<Map<String, Object>> dataList = new ArrayList<>();
                    dataList.add(map);
                    dataList.add(map1);
                    Map<String, Object> mm = new HashMap<>();
                    mm.put("titles", titles);
                    mm.put("values", dataList);
                    mm.put("name", "汇总");
                    maps.add(0, mm);
                    WriterExcelUtil.writerExcel(path, maps);


                }

            }

            smdbDruid.close(connection, s, rs);

            bfdbCon.close();
            stdbCon.close();
            hadbCon.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> getResultSetToMap(ResultSet rs, String flag) {
        List<Map<String, Object>> list = new ArrayList<>();
        // 获得结果集结构信息（元数据）
        ResultSetMetaData md = null;
        try {
            md = rs.getMetaData();
            // ResultSet列数
            int columnCount = md.getColumnCount();
            // ResultSet转List<Map>数据结构
            // next用于移动到ResultSet的下一行，使下一行成为当前行
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                // 遍历获取对当前行的每一列的键值对，put到map中
                for (int i = 1; i <= columnCount; i++) {
                    // rs.getObject(i) 获得当前行某一列字段的值
                    map.put(flag + "_" + md.getColumnName(i).toUpperCase(), rs.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;

    }

    public static List<String> getColumnNameList(ResultSet rs, String flag) {
        List<String> list = new ArrayList<>();
        // 获得结果集结构信息（元数据）
        ResultSetMetaData md = null;
        try {
            md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                list.add(flag + "_" + md.getColumnName(i).toUpperCase());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
