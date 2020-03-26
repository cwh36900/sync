package com.piesat.sod.job.job;

import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.EleWarningEntity;
import com.piesat.sod.sync.utils.DruidUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class SyncRunnable implements Runnable {

    private EleWarningEntity ew;

    Connection connection = null;
    Statement s = null;
    ResultSet rs = null;

    public SyncRunnable(EleWarningEntity ew) {
        this.ew = ew;
    }

    private String sql = "SELECT COUNT(*) C FROM %s.%s WHERE D_DATETIME >= '%s' AND D_DATETIME < '%s' ";

    @Override
    public void run() {
        System.out.println("任务一----" + ew.getTaskName() + ">>>>>>>>>>>" + new Date());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {
            LocalDateTime nowTime = LocalDateTime.now();
            String nowStr = nowTime.format(formatter);
            BigDecimal timeLimit = ew.getTimeLimit();
            BigDecimal[] bigDecimals = timeLimit.divideAndRemainder(BigDecimal.valueOf(1));
            int H = bigDecimals[0].intValue();

            LocalDateTime befTime = nowTime.plus(-H, ChronoUnit.HOURS);
            String befStr = befTime.format(formatter);
            DruidUtils sourceDruid = DruidBuilder.Builder(ew.getSourceDatabaseId());
            connection = sourceDruid.getConnection();
            s = connection.createStatement();
            String sourceSql = String.format(sql, sourceDruid.getDatabaseSchema(), ew.getSourceTableName(), befStr, nowStr);
            rs = s.executeQuery(sourceSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                System.out.println(">>>>>>>>>>>>>>>>>>" + sourceSql + ">>>>" + c);
            }
            sourceDruid.close(connection, s, rs);

            DruidUtils targetDruid = DruidBuilder.Builder(ew.getTargetDatabaseId());
            connection = targetDruid.getConnection();
            s = connection.createStatement();
            String targetSql = String.format(sql, targetDruid.getDatabaseSchema(), ew.getTargetTableName(), befStr, nowStr);
            rs = s.executeQuery(targetSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                System.out.println(">>>>>>>>>>>>>>>>>>" + sourceSql + ">>>>" + c);
            }
            targetDruid.close(connection, s, rs);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        DruidUtils targetDruid = DruidBuilder.Builder(ew.getTargetDatabaseId());

    }
}
