package com.piesat.sod.sync.entity;

import lombok.Data;

/**
 * 数据库信息
 *
 * @author cwh
 * @date 2020年 03月11日 10:04:25
 */
@Data
public class DatabaseEntity {
 private String databaseId;
 private String databaseName;
 private String driverClass;
 private String databaseInstance;
 private String databaseSchema;
 private String databaseUrl;
 private String databaseUser;
 private String databasePassword;
}
