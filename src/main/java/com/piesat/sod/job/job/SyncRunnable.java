package com.piesat.sod.job.job;

import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.EleWarningEntity;
import com.piesat.sod.sync.utils.DruidUtils;
import com.piesat.sod.sync.utils.EiParam;
import com.piesat.sod.sync.utils.EiSender;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

@Slf4j
public class SyncRunnable implements Runnable {

    private EleWarningEntity ew;

    public SyncRunnable(EleWarningEntity ew) {
        this.ew = ew;
    }

    private String sql = "SELECT COUNT(*) C FROM %s.%s WHERE D_DATETIME >= '%s' AND D_DATETIME < '%s' ";

    @Override
    public void run() {
        Connection connection = null;
        Statement s = null;
        ResultSet rs = null;

        DruidUtils sourceDruid = null;

        DruidUtils targetDruid = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        log.info(String.format("任务_%s_%s_%s", ew.getTaskName(), ew.getExecIp(), LocalDateTime.now().format(formatter)));
        try {
            LocalDateTime nowTime = LocalDateTime.now();
            nowTime = nowTime.plusMinutes(-2);
            String nowStr = nowTime.format(formatter);
            BigDecimal timeLimit = ew.getTimeLimit();
            BigDecimal[] bigDecimals = timeLimit.divideAndRemainder(BigDecimal.valueOf(1));
            int H = bigDecimals[0].intValue();

            int sourceCount = 0;
            int targetCount = 0;
            LocalDateTime befTime = nowTime.plus(-H, ChronoUnit.HOURS);
            String befStr = befTime.format(formatter);
            sourceDruid = DruidBuilder.Builder(ew.getSourceDatabaseId());
            connection = sourceDruid.getConnection();
            s = connection.createStatement();
            String sourceSql = String.format(sql, sourceDruid.getDatabaseSchema(), ew.getSourceTableName(), befStr, nowStr);
            rs = s.executeQuery(sourceSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                sourceCount = Integer.parseInt(c.toString());
            }

            sourceDruid.close(connection, s, rs);

            targetDruid = DruidBuilder.Builder(ew.getTargetDatabaseId());
            connection = targetDruid.getConnection();
            s = connection.createStatement();
            String targetSql = String.format(sql, targetDruid.getDatabaseSchema(), ew.getTargetTableName(), befStr, nowStr);
            rs = s.executeQuery(targetSql);
            while (rs.next()) {
                Object c = rs.getObject("C");
                targetCount = Integer.parseInt(c.toString());
            }

            targetDruid.close(connection, s, rs);

            int diff = sourceCount - targetCount;
            if (diff > ew.getBiggestDifference()) {
                log.info("sourceSql:" + sourceSql);
                log.info("targetSql:" + targetSql);
                EiSender.sendEI(EiParam.type,
                        String.format("数据同步_%s_数据同步缺失", ew.getTaskName()),
                        String.format("%s.%s>>%s.%s-数据表", ew.getSourceDatabaseId(), ew.getSourceTableName(),
                                ew.getTargetDatabaseId(), ew.getTargetTableName()),
                        String.format("同步节点：%s,要素表数据同步缺失，缺失条数：%s", ew.getExecIp(), diff),
                        "要素表数据同步缺失", String.format("同步任务：%s", ew.getTaskName()),
                        "SOD", String.format("节点：%s", ew.getExecIp()));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            sourceDruid.close(connection, s, rs);
            targetDruid.close(connection, s, rs);
        }
    }
}
