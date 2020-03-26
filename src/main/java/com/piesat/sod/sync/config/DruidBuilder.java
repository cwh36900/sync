package com.piesat.sod.sync.config;

import com.piesat.sod.sync.entity.DatabaseEntity;
import com.piesat.sod.sync.utils.DruidUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author cwh
 * @date 2020年 03月24日 18:03:14
 */
public class DruidBuilder {
    public static Map<String, DatabaseEntity> druidConfigMap = new HashMap<>();

    public static DruidUtils Builder(String databaseId){
        DatabaseEntity databaseEntity = druidConfigMap.get(databaseId);
        if (databaseEntity == null){
            return null;
        }else {
            DruidUtils du = new DruidUtils(databaseEntity);
            return du;
        }
    }
}
