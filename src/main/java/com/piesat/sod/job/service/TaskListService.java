package com.piesat.sod.job.service;

import com.piesat.sod.job.bean.TaskList;
import com.piesat.sod.sync.entity.EleWarningEntity;
import com.piesat.sod.sync.mapper.EleWarningMapper;
import com.piesat.sod.sync.utils.CronUtils;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Service
public class TaskListService {

	@Autowired
	private EleWarningMapper eleWarningMapper;



	@Transactional
	public void save(TaskList taskList) {

	}

	public void updateProxyCronById(String cron, String id){
	}

	public Optional<TaskList> findById(String id) {
		return null;
	}

	public List<TaskList> getEleWarning(){
		List<EleWarningEntity> syncEleWarning = this.eleWarningMapper.getSyncEleWarning();
		List<TaskList> l = new ArrayList<>();
		for (EleWarningEntity e:syncEleWarning) {
			TaskList tl = new TaskList();

			tl.setId(e.getTaskId());
			tl.setStatus(1);
			tl.setTaskname(e.getTaskName());
			tl.setCron(CronUtils.getCron(e.getTimeInterval()));
			tl.setCreatetime(new Date());
			tl.setClazz("com.piesat.sod.job.job.SyncRunnable");
			tl.setEleWarningEntity(e);
			l.add(tl);
		}
		return l;
	}

}
