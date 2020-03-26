package com.piesat.sod.job.service;
import com.piesat.sod.sync.entity.DatabaseEntity;
import com.piesat.sod.sync.mapper.DatabaseMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.List;

/**
 * 数据库服务
 *
 * @author cwh
 * @date 2020年 03月24日 13:28:33
 */
@Service
public class DatabaseService {
    @Autowired
    private DatabaseMapper databaseMapper;

    public List<DatabaseEntity> getDatabaseInfo(){
        return this.databaseMapper.getDatabaseInfo();
    }
}
