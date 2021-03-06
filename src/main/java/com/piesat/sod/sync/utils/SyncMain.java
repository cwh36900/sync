package com.piesat.sod.sync.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.piesat.sod.job.dto.Log;
import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author cwh
 * @date 2020年 04月27日 19:00:32
 */
@Slf4j
public class SyncMain {

    static Map<String, Connection> connection = new HashMap<>();

    static DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static DateTimeFormatter DTF1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    static final String SMDB = "SMDB";
    static final String STDB = "STDB";
    static final String HADB = "HADB";
    static final String BFDB = "BFDB";
    static final String MIN = "MIN";
    static final String HOR = "HOR";
    static final String COUNT = "COUNT";
    static final String SQL = "SQL";

    static final long ERROR_COUNT = -9999999;


    static final String SCHEMA = "USR_SOD";

    static final String DB_INFO_SQL = "SELECT B.DATABASE_DEFINE_ID S_DB_ID,C.DATABASE_DEFINE_ID T_DB_ID,SOURCE_TABLE,TASK_NAME FROM T_SOD_JOB_SYNCTASK_INFO A LEFT JOIN T_SOD_DATABASE B ON A.SOURCE_DATABASE_ID = B.ID LEFT JOIN T_SOD_DATABASE C ON A.TARGET_DATABASE_ID = C.ID";

    static final String TABLE_INFO_SQL = "SELECT SOURCE_TABLE_NAME,TARGET_TABLE_NAME FROM T_SOD_JOB_SYNCMAPPING_INFO WHERE ID = %s";

    static String COUNT_SQL = "SELECT COUNT(*) C FROM %s.%s WHERE D_DATETIME >= '%s' AND D_DATETIME < '%s' ";

    static String smdbUrl = "";
    static String smdbUser = "";
    static String smdbPwd = "";
    static String bfdbUrl = "";
    static String bfdbUser = "";
    static String bfdbPwd = "";
    static String stdbUrl = "";
    static String stdbUser = "";
    static String stdbPwd = "";
    static String hadbUrl = "";
    static String hadbUser = "";
    static String hadbPwd = "";

    public static void loadConf(String path) throws Exception {
        Properties props = new Properties();
//        InputStream in = new FileInputStream(path);
        InputStream in = SyncAccuracyMain.class.getResourceAsStream("/db.config");
        props.load(in);
        in.close();

        smdbUrl = props.getProperty("smdbUrl");
        smdbUser = props.getProperty("smdbUser");
        smdbPwd = props.getProperty("smdbPwd");

        bfdbUrl = props.getProperty("bfdbUrl");
        bfdbUser = props.getProperty("bfdbUser");
        bfdbPwd = props.getProperty("bfdbPwd");

        stdbUrl = props.getProperty("stdbUrl");
        stdbUser = props.getProperty("stdbUser");
        stdbPwd = props.getProperty("stdbPwd");

        hadbUrl = props.getProperty("hadbUrl");
        hadbUser = props.getProperty("hadbUser");
        hadbPwd = props.getProperty("hadbPwd");

    }

    static {
        try {
            loadConf("db.config");
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            System.exit(1);
            System.out.println("配置有误");
        }
        DatabaseEntity smdb = new DatabaseEntity();
        smdb.setDatabaseId(SMDB);
        smdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        smdb.setDatabaseUrl(smdbUrl);
        smdb.setDatabaseUser(smdbUser);
        smdb.setDatabasePassword(smdbPwd);
        DruidBuilder.druidConfigMap.put(SMDB, smdb);
        DatabaseEntity bfdb = new DatabaseEntity();
        bfdb.setDatabaseId(BFDB);
        bfdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        bfdb.setDatabaseUrl(bfdbUrl);
        bfdb.setDatabaseUser(bfdbUser);
        bfdb.setDatabasePassword(bfdbPwd);
        DruidBuilder.druidConfigMap.put(BFDB, bfdb);
        DatabaseEntity stdb = new DatabaseEntity();
        stdb.setDatabaseId(STDB);
        stdb.setDriverClass("com.xugu.cloudjdbc.Driver");
        stdb.setDatabaseUrl(stdbUrl);
        stdb.setDatabaseUser(stdbUser);
        stdb.setDatabasePassword(stdbPwd);
        DruidBuilder.druidConfigMap.put(STDB, stdb);
        DatabaseEntity hadb = new DatabaseEntity();
        hadb.setDatabaseId(HADB);
        hadb.setDriverClass("com.gbase.jdbc.Driver");
        hadb.setDatabaseUrl(hadbUrl);
        hadb.setDatabaseUser(hadbUser);
        hadb.setDatabasePassword(hadbPwd);
        DruidBuilder.druidConfigMap.put(HADB, hadb);

    }

    public static void main(String[] args) throws Exception {
        try {
            DruidUtils smdbDruid = DruidBuilder.Builder(SMDB);
            Connection smdbConnection = smdbDruid.getConnection();
            connection.put(SMDB, smdbConnection);
            DruidUtils stdbDruid = DruidBuilder.Builder(STDB);
            Connection stdbConnection = stdbDruid.getConnection();
            connection.put(STDB, stdbConnection);
            DruidUtils bfdbDruid = DruidBuilder.Builder(BFDB);
            Connection bfdbConnection = bfdbDruid.getConnection();
            connection.put(BFDB, bfdbConnection);
            DruidUtils hadbDruid = DruidBuilder.Builder(HADB);
            Connection hadbConnection = hadbDruid.getConnection();
            connection.put(HADB, hadbConnection);
            Statement statement = connection.get(SMDB).createStatement();
            ResultSet rs = statement.executeQuery(DB_INFO_SQL);
            List<Map<String, String>> l = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> m = new HashMap<>();
                String SOURCE_TABLE = rs.getString("SOURCE_TABLE");
                String TASK_NAME = rs.getString("TASK_NAME");
                String S_DB_ID = rs.getString("S_DB_ID");
                String T_DB_ID = rs.getString("T_DB_ID");
                m.put("TASK_NAME", TASK_NAME);
                m.put("SOURCE_TABLE", SOURCE_TABLE);
                m.put("S_DB_ID", S_DB_ID);
                m.put("T_DB_ID", T_DB_ID);
                l.add(m);
            }

            rs.close();
            List<SyncInfo> siList = new ArrayList<>();
            l.forEach(ss -> {

                String source_tables = ss.get("SOURCE_TABLE");
                String[] split = source_tables.split(",");
                for (String s1 : split) {
                    String formatSQL = String.format(TABLE_INFO_SQL, s1);
                    try {

                        ResultSet rs1 = statement.executeQuery(formatSQL);
                        while (rs1.next()) {
                            SyncInfo si = new SyncMain().new SyncInfo();
                            String source_table_name = rs1.getString("SOURCE_TABLE_NAME");
                            String target_table_name = rs1.getString("TARGET_TABLE_NAME");
                            si.setTaskName(ss.get("TASK_NAME"));
                            si.setSDbId(ss.get("S_DB_ID"));
                            si.setTDbId(ss.get("T_DB_ID"));
                            si.setSourceTableName(source_table_name);
                            si.setTargetTableName(target_table_name);
//                            if (!source_table_name.equalsIgnoreCase("SURF_WEA_GLB_MUL_HOR_TAB")&&!source_table_name.equalsIgnoreCase("OCEN_SHB_GLB_MUL_TAB")){
//                                continue;
//                            }
                            siList.add(si);
                        }
                        rs1.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
            statement.close();
            String root = System.getProperty("user.dir");
            //创建文本文件
            LocalDateTime dateTime = LocalDateTime.now();
            String fileName = String.format("%s_out.xlsx", DTF1.format(dateTime));
            List<Map<String, Object>> values = Lists.newArrayList();
            siList.forEach(e -> {
                String taskName = e.getTaskName();
                boolean etl = taskName.contains("ETL");
                String sDbId = e.getSDbId();
                String sourceTableName = e.getSourceTableName();
                LocalDateTime nowDateTime = LocalDateTime.now();
                nowDateTime = nowDateTime.plusDays(-1);
//                nowDateTime = nowDateTime.plusHours(-3);
                LocalDateTime endTime = nowDateTime;
                LocalDateTime beginTime;
                if (sourceTableName.toUpperCase().contains(MIN) || etl) {
                    beginTime = nowDateTime.plusMinutes(-10);
                } else if (sourceTableName.toUpperCase().contains(HOR)) {
                    beginTime = nowDateTime.plusHours(-12);
                } else {
                    beginTime = nowDateTime.plusDays(-1);
                }
//                String beginStr = DTF.format(beginTime);
//                String endStr = DTF.format(endTime);
                String beginStr = "2020-07-21 00:00:00";
                String endStr = "2020-08-20 00:00:00";
                System.out.println("查询" + sDbId + "." + SCHEMA + "." + sourceTableName);
                Map<String, Object> sm = getCount(sDbId, sourceTableName, beginStr, endStr, etl);
                long sCount = (long) sm.get(COUNT);
                String sSql = sm.get(SQL).toString();
                String tDbId = e.getTDbId();
                String targetTableName = e.getTargetTableName();
                System.out.println("查询" + tDbId + "." + SCHEMA + "." + targetTableName);
                Map<String, Object> tm = getCount(tDbId, targetTableName, beginStr, endStr, etl);
                long tCount = (long) tm.get(COUNT);
                String tSql = tm.get(SQL).toString();
                Map<String, Object> map = Maps.newHashMap();
                map.put("任务", e.getTaskName());
                map.put("源库", sDbId);
                map.put("源表", sourceTableName);
                map.put("源表数据量", sCount);
                map.put("源表SQL", sSql);
                map.put("目标库", tDbId);
                map.put("目标表", targetTableName);
                map.put("目标表据量", tCount);
                map.put("目标表SQL", tSql);
                map.put("差值", sCount - tCount);
                values.add(map);
                String info = String.format("任务：%s\t 源库:%s\t 源表：%s\t %s\t 目标库:%s\t 目标表：%s\t %s\t 差值:%s\t",
                        String.format("%-45s", e.getTaskName()),
                        sDbId,
                        String.format("%-30s", sourceTableName),
                        String.format("%-10s", sCount),
                        tDbId,
                        String.format("%-30s", targetTableName),
                        String.format("%-10s", tCount),
                        String.format("%-10s", sCount - tCount));

                System.out.println(info);

            });

            String path = root + File.separator + fileName;
            String name = "同步量";
            List<String> titles = Lists.newArrayList();
            titles.add("任务");
            titles.add("源库");
            titles.add("源表");
            titles.add("源表数据量");
            titles.add("源表SQL");
            titles.add("目标库");
            titles.add("目标表");
            titles.add("目标表据量");
            titles.add("目标表SQL");
            titles.add("差值");
            WriterExcelUtil.writerExcel(path, name, titles, values);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.forEach((k, v) -> {
                if (v != null) {
                    try {
                        v.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public static Map<String, Object> getCount(String dbId, String tableName, String beginStr, String endStr, Boolean etl) {

        if (dbId == null) {
            Map<String, Object> m = new HashMap<>();
            m.put(COUNT, ERROR_COUNT);
            m.put(SQL, "数据库不存在");
            return m;
        } else if (tableName.toUpperCase().contains("TEST")) {
            Map<String, Object> m = new HashMap<>();
            m.put(COUNT, ERROR_COUNT);
            m.put(SQL, tableName.toUpperCase() + "表为测试表，不做统计");
            return m;
        } else if (dbId.toUpperCase().equals(STDB)) {
            return getStdbCount(tableName, beginStr, endStr);
        } else if (dbId.toUpperCase().equals(BFDB)) {
            return getBfdbCount(tableName, beginStr, endStr);
        } else if (dbId.toUpperCase().equals(HADB)) {
            return getHadbCount(tableName, beginStr, endStr);
        } else {
            Map<String, Object> m = new HashMap<>();
            m.put(COUNT, ERROR_COUNT);
            m.put(SQL, "未知错误");
            return m;
        }
    }

    public static Map<String, Object> getStdbCount(String tableName, String beginStr, String endStr) {
        long count = ERROR_COUNT;
        Statement statement = null;
        ResultSet rs = null;
        String stdbSql = String.format(COUNT_SQL, SCHEMA, tableName, beginStr, endStr);
        try {
            statement = connection.get(STDB).createStatement();
            rs = statement.executeQuery(stdbSql);
            while (rs.next()) {
                count = rs.getLong(1);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        Map<String, Object> m = new HashMap<>();
        m.put(COUNT, count);
        m.put(SQL, stdbSql);
        return m;
    }

    public static Map<String, Object> getBfdbCount(String tableName, String beginStr, String endStr) {
        long count = ERROR_COUNT;
        Statement statement = null;
        ResultSet rs = null;
        String bfdbSql = String.format(COUNT_SQL, SCHEMA, tableName, beginStr, endStr);
        try {
            statement = connection.get(BFDB).createStatement();
            rs = statement.executeQuery(bfdbSql);
            while (rs.next()) {
                count = rs.getLong(1);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        Map<String, Object> m = new HashMap<>();
        m.put(COUNT, count);
        m.put(SQL, bfdbSql);
        return m;
    }

    public static Map<String, Object> getHadbCount(String tableName, String beginStr, String endStr) {
        long count = ERROR_COUNT;
        Statement statement = null;
        ResultSet rs = null;
        String hadbSql = String.format(COUNT_SQL, SCHEMA, tableName, beginStr, endStr);
        try {
            statement = connection.get(HADB).createStatement();
            rs = statement.executeQuery(hadbSql);
            while (rs.next()) {
                count = rs.getLong(1);
            }
            rs.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        Map<String, Object> m = new HashMap<>();
        m.put(COUNT, count);
        m.put(SQL, hadbSql);
        return m;
    }


    @Data
    public class SyncInfo {
        private String taskName;
        private String sDbId;
        private String tDbId;
        private String sourceTableName;
        private String targetTableName;
    }
}
