package com.piesat.sod.sync.mapper;

import com.piesat.sod.sync.entity.EleWarningEntity;

import java.util.List;

/**
 * @author cwh
 * @date 2020年 03月24日 14:15:06
 */
public interface EleWarningMapper {
    List<EleWarningEntity> getSyncEleWarning();
}
