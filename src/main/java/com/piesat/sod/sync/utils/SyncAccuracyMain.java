package com.piesat.sod.sync.utils;

import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author cwh
 * @date 2020年 04月27日 19:00:32
 */
@Slf4j
public class SyncAccuracyMain {

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

    static final String TABLE_INFO_SQL = "SELECT SOURCE_TABLE_NAME,TARGET_TABLE_NAME,TARGET_TABLE_ID FROM T_SOD_JOB_SYNCMAPPING_INFO WHERE ID = %s";

    static final String TABLE_UNIQUE_SQL = "SELECT UNIQUE_KEYS FROM T_SOD_JOB_SYNCCONFIG_INFO WHERE ID = %s";

    static String SOURCE_INFO_SQL = "SELECT * FROM %s.%s WHERE D_DATETIME >= '%s' AND D_DATETIME < '%s' LIMIT 30";

    static String TARGET_INFO_SQL = "SELECT * FROM %s.%s WHERE %s";

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
                            SyncInfo si = new SyncAccuracyMain().new SyncInfo();
                            String source_table_name = rs1.getString("SOURCE_TABLE_NAME");
                            String target_table_name = rs1.getString("TARGET_TABLE_NAME");
                            String target_table_id = rs1.getString("TARGET_TABLE_ID");
                            String tableUniqueSql = String.format(TABLE_UNIQUE_SQL, target_table_id);
                            ResultSet resultSet = statement.executeQuery(tableUniqueSql);
                            if (resultSet.next()) {
                                String uniqueKeys = resultSet.getString(1);
                                List<String> keys = Arrays.stream(uniqueKeys.split(",")).collect(Collectors.toList());
                                si.setUniqueKeys(keys);
                            }
                            si.setTaskName(ss.get("TASK_NAME"));
                            si.setSDbId(ss.get("S_DB_ID"));
                            si.setTDbId(ss.get("T_DB_ID"));
                            si.setSourceTableName(source_table_name);
                            si.setTargetTableName(target_table_name);
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
            List<Map<String, Object>> contentValues = Lists.newArrayList();
            siList.forEach(e -> {
                String taskName = e.getTaskName();
                String sDbId = e.getSDbId();
                String sourceTableName = e.getSourceTableName();
                String tDbId = e.getTDbId();
                String targetTableName = e.getTargetTableName();
//                if (targetTableName.contains("TEST") || taskName.contains("ETL")) {
//                    return;
//                }
//                if (!taskName.contains("高空") && !taskName.contains("地面") && !taskName.contains("海洋")) {
//                    return;
//                }
                LocalDateTime nowDateTime = LocalDateTime.now();
                nowDateTime = nowDateTime.plusDays(-3);
                LocalDateTime endTime = nowDateTime;
                LocalDateTime beginTime;
                if (sourceTableName.toUpperCase().contains(MIN)) {
                    beginTime = nowDateTime.plusMinutes(-20);
                } else if (sourceTableName.toUpperCase().contains(HOR)) {
                    beginTime = nowDateTime.plusHours(-62);
                } else {
                    beginTime = nowDateTime.plusDays(-15);
                }
                String beginStr = DTF.format(beginTime);
                String endStr = DTF.format(endTime);
                System.out.println("查询" + sDbId + "." + SCHEMA + "." + sourceTableName);

                List<Map<String, Object>> sourceData = getSourceData(sDbId, sourceTableName, beginStr, endStr);

                List<String> uniqueKeys = e.getUniqueKeys();
                if (sourceData.size() == 0) {
                    return;
                }

                Map<String, Object> map = Maps.newHashMap();
                map.put("任务", taskName);
                map.put("源库", sDbId);
                map.put("源表", sourceTableName);
                map.put("目标库", tDbId);
                map.put("目标表", targetTableName);

                List<String> tabCls = new ArrayList<>();

                int same = 0;
                for (int i = 0; i < sourceData.size(); i++) {
                    Map<String, Object> u = sourceData.get(i);
                    List<Map<String, Object>> targetData = getTargetData(tDbId, targetTableName, uniqueKeys, u);
                    if (targetData.size() > 0) {
                        Map<String, Object> t = targetData.get(0);
                        boolean f = u.keySet().size() > t.keySet().size();
                        boolean b = f ? compareMap(u, t) : compareMap(t, u);
                        List<String> cls = new ArrayList<>();
                        u.keySet().stream().forEach(r -> {
                            String sData = getData(u.get(r));
                            String tData = getData(targetData.get(0).get(r));
                            String cl = String.format("[N：%s,S:%s,T:%s]", r, sData, tData);
                            cls.add(cl);
                        });
                        String clStr = cls.stream().collect(Collectors.joining(""));
                        Map<String, Object> contentMap = Maps.newHashMap();
                        contentMap.put("任务", taskName);
                        contentMap.put("源库", sDbId);
                        contentMap.put("源表", sourceTableName);
                        contentMap.put("目标库", tDbId);
                        contentMap.put("目标表", targetTableName);
                        contentMap.put("条数", String.format("第%s条", i + 1));
                        contentMap.put("内容", clStr);
                        contentValues.add(contentMap);
                        String lineSeparator = System.lineSeparator();
                        tabCls.add(String.format("{%s}%s", clStr, lineSeparator));
                        if (b) {
                            same++;
                        }else {

                        }
                    }
                }
                String collect = tabCls.stream().collect(Collectors.joining(""));
                map.put("抽查数据量", sourceData.size());
                map.put("完全一致数据量", same);
                map.put("不一致数据量", sourceData.size() - same);
                System.out.println(collect);
                values.add(map);
            });

            String path = root + File.separator + fileName;
            String name = "准确性对比";
            List<String> titles = Lists.newArrayList();
            titles.add("任务");
            titles.add("源库");
            titles.add("源表");
            titles.add("目标库");
            titles.add("目标表");
            titles.add("抽查数据量");
            titles.add("完全一致数据量");
            titles.add("不一致数据量");

            String contentName = "数据内容";
            List<String> contentTitles = Lists.newArrayList();
            contentTitles.add("任务");
            contentTitles.add("源库");
            contentTitles.add("源表");
            contentTitles.add("目标库");
            contentTitles.add("目标表");
            contentTitles.add("条数");
            contentTitles.add("内容");

            List<Map<String, Object>> maps = new ArrayList<>();
            Map<String, Object> m = new HashMap<>();
            m.put("name", name);
            m.put("titles", titles);
            m.put("values", values);
            maps.add(m);
            Map<String, Object> m1 = new HashMap<>();
            m1.put("name", contentName);
            m1.put("titles", contentTitles);
            m1.put("values", contentValues);
            maps.add(m1);
            WriterExcelUtil.writerExcel(path, maps);
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

    public static List<Map<String, Object>> getSourceData(String dbId, String tableName, String beginStr, String endStr) {
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = String.format(SOURCE_INFO_SQL, SCHEMA, tableName, beginStr, endStr);
        if (dbId == null) {
            return list;
        } else if (tableName.toUpperCase().contains("TEST")) {
            return list;
        } else if (dbId.toUpperCase().equals(STDB)) {
            return executeStdbSql(sql);
        } else if (dbId.toUpperCase().equals(BFDB)) {
            return executeBfdbSql(sql);
        } else if (dbId.toUpperCase().equals(HADB)) {
            return executeHadbSql(sql);
        } else {
            return list;
        }
    }


    public static List<Map<String, Object>> getTargetData(String dbId, String tableName, List<String> uniqueKeys, Map<String, Object> sourceData) {
        List<Map<String, Object>> list = new ArrayList<>();
        String where = uniqueKeys.stream().map(e -> {
            Object o = sourceData.get(e);
            String dataString = getDataString(o);
            return e + " = '" + dataString + "'";
        }).collect(Collectors.joining(" AND "));

        String sql = String.format(TARGET_INFO_SQL, SCHEMA, tableName, where);
        if (dbId == null) {
            return list;
        } else if (tableName.toUpperCase().contains("TEST")) {
            return list;
        } else if (dbId.toUpperCase().equals(STDB)) {
            return executeStdbSql(sql);
        } else if (dbId.toUpperCase().equals(BFDB)) {
            return executeBfdbSql(sql);
        } else if (dbId.toUpperCase().equals(HADB)) {
            return executeHadbSql(sql);
        } else {
            return list;
        }
    }


    public static String getDataString(Object o) {
        if (o instanceof Date || o instanceof java.sql.Date) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return simpleDateFormat.format(o);
        } else {
            if (o == null) {
                return "";
            } else {
                return o.toString();
            }

        }
    }

    public static String getData(Object o) {
        if (o instanceof Date || o instanceof java.sql.Date) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return simpleDateFormat.format(o);
        } else if (o instanceof java.math.BigDecimal) {
            return ((BigDecimal) o).stripTrailingZeros().toString().trim();
        } else {
            if (o != null) {
                return o.toString().trim();
            } else {
                return "";
            }
        }

    }

    public static List<Map<String, Object>> executeStdbSql(String executeSql) {
        Statement statement = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            statement = connection.get(STDB).createStatement();
            rs = statement.executeQuery(executeSql);
            list = getResultSetToMap(rs);
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
        return list;
    }

    public static List<Map<String, Object>> executeBfdbSql(String executeSql) {
        Statement statement = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            statement = connection.get(BFDB).createStatement();
            rs = statement.executeQuery(executeSql);
            list = getResultSetToMap(rs);
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
        return list;
    }

    public static List<Map<String, Object>> executeHadbSql(String executeSql) {
        Statement statement = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            statement = connection.get(HADB).createStatement();
            rs = statement.executeQuery(executeSql);
            list = getResultSetToMap(rs);
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
        return list;
    }

    public static List<Map<String, Object>> getResultSetToMap(ResultSet rs) {
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
                    map.put(md.getColumnName(i).toUpperCase(), rs.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;

    }

    public static boolean compareMap(Map<String, Object> sourceMap, Map<String, Object> targetMap) {
        return !sourceMap.keySet().stream()
                .anyMatch(e ->
                        sourceMap.get(e) != null
                                && !e.toUpperCase().equals("D_IYMDHM")
                                && !e.toUpperCase().equals("D_UPDATE_TIME")
                                && !getData(sourceMap.get(e)).equals(getData(targetMap.get(e))));
    }

    @Data
    public class SyncInfo {
        private String taskName;
        private String sDbId;
        private String tDbId;
        private String sourceTableName;
        private String targetTableName;
        private List<String> uniqueKeys;
    }
}
