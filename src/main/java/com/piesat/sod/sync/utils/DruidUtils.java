package com.piesat.sod.sync.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.piesat.sod.sync.entity.DatabaseEntity;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * 数据库连接池工具类
 *
 * @author cwh
 * @date 2020年 03月11日 10:52:25
 */
public class DruidUtils {
    // 1. 声明静态数据源成员变量
    private DataSource ds;
    // 2. 创建连接池对象
    Properties pp = new Properties();


    private String databaseSchema;

    public String getDatabaseSchema() {
        return databaseSchema;
    }

    public DruidUtils(DatabaseEntity config){
        // 加载配置文件中的数据
        InputStream is = DruidUtils.class.getResourceAsStream("/druid.properties");
        try {
            pp.load(is);
            pp.put("driverClassName",config.getDriverClass());
            pp.put("url",config.getDatabaseUrl());
            pp.put("username",config.getDatabaseUser());
            pp.put("password",config.getDatabasePassword());
            ds = DruidDataSourceFactory.createDataSource(pp);
            this.databaseSchema = config.getDatabaseSchema();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // 3. 定义公有的得到数据源的方法
    public DataSource getDataSource() {
        return ds;
    }

    // 4. 定义得到连接对象的方法
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    // 5.定义关闭资源的方法
    public void close(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }
    }

    // 6.重载关闭方法
    public void close(Connection conn, Statement stmt) {
        close(conn, stmt, null);
    }
}
