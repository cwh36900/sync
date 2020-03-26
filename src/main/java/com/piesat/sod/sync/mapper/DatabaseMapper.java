package com.piesat.sod.sync.mapper;

import com.piesat.sod.sync.entity.DatabaseEntity;

import java.util.List;

/**
 * @author cwh
 * @date 2020年 03月11日 13:30:05
 */
public interface DatabaseMapper {
    List<DatabaseEntity> getDatabaseInfo();
}
