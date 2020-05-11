package com.piesat.sod.sync.utils;

import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;
import com.piesat.sod.sync.entity.EleWarningEntity;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cwh
 * @date 2020年 04月27日 19:00:32
 */
public class SyncMain {

    static Connection connection = null;
    static Statement s = null;
    static ResultSet rs = null;

    static String sql = "SELECT COUNT(*) C FROM %s.%s WHERE D_DATETIME >= '%s' AND D_DATETIME < '%s' ";

    public static void main(String[] args) {


        if (args.length != 4 ){
            System.out.println("参数不正确");
            return;
        }
        String schema = args[0];
        String tableName = args[1];
        String beginStr = args[2];
        String endStr = args[3];


        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {
            DatabaseEntity bfdb = new DatabaseEntity();
            bfdb.setDatabaseId("bfdb");
            bfdb.setDriverClass("com.xugu.cloudjdbc.Driver");
            bfdb.setDatabaseUrl("jdbc:xugu://10.20.64.41:5138/BABJ_BFDB?ips=10.20.64.43&char_set=utf8&recv_mode=0");
            bfdb.setDatabaseUser("usr_manager");
            bfdb.setDatabasePassword("manager_123");
            DruidBuilder.druidConfigMap.put("bfdb", bfdb);
            DatabaseEntity stdb = new DatabaseEntity();
            stdb.setDatabaseId("stdb");
            stdb.setDriverClass("com.xugu.cloudjdbc.Driver");
            stdb.setDatabaseUrl("jdbc:xugu://10.20.64.38:5138/BABJ_STDB?ips=10.20.64.39,10.20.64.40,10.20.64.47,10.20.64.51,10.20.64.52&char_set=utf8&recv_mode=2");
            stdb.setDatabaseUser("usr_manager");
            stdb.setDatabasePassword("manager_123");
            DruidBuilder.druidConfigMap.put("stdb", stdb);
            DatabaseEntity hadb = new DatabaseEntity();
            hadb.setDatabaseId("hadb");
            hadb.setDriverClass("com.gbase.jdbc.Driver");
            hadb.setDatabaseUrl("jdbc:gbase://10.20.64.29:5258/usr_sod?useOldAliasMetadataBehavior=true&rewriteBatchedStatements=true&connectTimeout=0&hostList=10.20.64.29,10.20.64.30,10.20.64.31&failoverEnable=true");
            hadb.setDatabaseUser("usr_manager");
            hadb.setDatabasePassword("manager_123");
            DruidBuilder.druidConfigMap.put("hadb", hadb);


            DruidUtils bfdbDruid = DruidBuilder.Builder("bfdb");
            connection = bfdbDruid.getConnection();
            s = connection.createStatement();
            String bfdbSql = String.format(sql, schema, tableName, beginStr, endStr);
            rs = s.executeQuery(bfdbSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                int count = Integer.parseInt(c.toString());
                System.out.println("缓冲库:"+count);
            }
            bfdbDruid.close(connection, s, rs);

            DruidUtils stdbDruid = DruidBuilder.Builder("stdb");
            connection = stdbDruid.getConnection();
            s = connection.createStatement();
            String stdbSql = String.format(sql, schema, tableName, beginStr, endStr);
            rs = s.executeQuery(stdbSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                int count = Integer.parseInt(c.toString());
                System.out.println("服务库:"+count);
            }
            stdbDruid.close(connection, s, rs);

            DruidUtils hadbDruid = DruidBuilder.Builder("hadb");
            connection = hadbDruid.getConnection();
            s = connection.createStatement();
            String hadbSql = String.format(sql, schema, tableName, beginStr, endStr);
            rs = s.executeQuery(hadbSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                int count = Integer.parseInt(c.toString());
                System.out.println("分析库:"+count);
            }
            hadbDruid.close(connection, s, rs);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
