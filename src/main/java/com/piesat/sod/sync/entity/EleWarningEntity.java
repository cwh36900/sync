package com.piesat.sod.sync.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author cwh
 * @date 2020年 03月24日 10:05:20
 */
@Data
public class EleWarningEntity {
    private String sourceDatabaseId;
    private String sourceTableName;
    private String targetDatabaseId;
    private String targetTableName;
    private String execIp;

    private String taskId;
    private String taskName;
    private BigDecimal timeInterval;
    private BigDecimal timeLimit;
    private Integer biggestDifference;
}
