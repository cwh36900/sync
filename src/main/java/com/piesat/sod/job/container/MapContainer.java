package com.piesat.sod.job.container;

import com.piesat.sod.job.bean.TaskList;
import com.piesat.sod.job.dto.TaskListDto;
import com.piesat.sod.sync.entity.EleWarningEntity;
import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
public class MapContainer {

	// 静态内部类实现单例模式
	private MapContainer() {
	}

	private static class MapContainerInstance {
		private static final MapContainer INSTANCE = new MapContainer();
	}

	public static MapContainer getInstance() {
		return MapContainerInstance.INSTANCE;
	}

	/**
	 * 启动后存储任务列表
	 */
	public final static Map<String, TaskListDto> currentHashMap = new ConcurrentHashMap<>();

	/**
	 * 工具类注入bean的方法
	 */
	@Autowired
	private ThreadPoolTaskScheduler threadPoolTaskScheduler;

	@PostConstruct
	public void init() {
		MapContainerInstance.INSTANCE.threadPoolTaskScheduler = this.threadPoolTaskScheduler;
	}

	/**
	 * 根据ID获取
	 *
	 * @param id
	 * @return
	 */
	public TaskListDto getById(Integer id) {
		return currentHashMap.get(id);
	}

	/**
	 * 返回执行的任务列表
	 *
	 * @return
	 */
	public Map<String, TaskListDto> getMapContainer() {
		return currentHashMap;
	}

	/**
	 * 将任务列表处理存入Map容器中
	 *
	 * @param taskList
	 * @return
	 */
	public TaskListDto putMap(TaskList taskList) {
		TaskListDto taskListDto = new TaskListDto();
		taskListDto.setCron(taskList.getCron());
		String clazz = taskList.getClazz();
		Object obj = null;
		try {
			obj = Class.forName(clazz).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			log.error("putMap时,反射实例化发生错误,错误原因- {}",e.getMessage());
		}
		taskListDto.setTask(obj);
		log.info("ThreadPoolTaskScheduler {}", threadPoolTaskScheduler);
		ScheduledFuture<?> future = threadPoolTaskScheduler.schedule((Runnable) obj,
				new CronTrigger(taskList.getCron()));
		taskListDto.setFuture(future);
		currentHashMap.put(taskList.getId(), taskListDto);
		log.info("TaskList的实例存储 - {}" , taskList);
		return taskListDto;
	}

	/**
	 * 将任务列表处理存入Map容器中
	 *
	 * @param taskList
	 * @return
	 */
	public TaskListDto putMap(TaskList taskList, EleWarningEntity ew) {
		TaskListDto taskListDto = new TaskListDto();
		taskListDto.setCron(taskList.getCron());
		String clazz = taskList.getClazz();
		Object obj = null;
		try {
			obj = Class.forName(clazz).getConstructor(EleWarningEntity.class).newInstance(ew);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			log.error("putMap时,反射实例化发生错误,错误原因- {}",e.getMessage());
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		taskListDto.setTask(obj);
		log.info("ThreadPoolTaskScheduler {}", threadPoolTaskScheduler);
		ScheduledFuture<?> future = threadPoolTaskScheduler.schedule((Runnable) obj,
				new CronTrigger(taskList.getCron()));
		taskListDto.setFuture(future);
		currentHashMap.put(taskList.getId(), taskListDto);
		log.info("TaskList的实例存储 - {}" , taskList);
		return taskListDto;
	}

	/**
	 * 根据ID暂停任务,同时在容器中删除
	 * @param id
	 * @return
	 */
	public TaskListDto cancelMap(String id) {
		TaskListDto task = null;
		if(currentHashMap.containsKey(id)){
			task = currentHashMap.get(id);
			ScheduledFuture<?> future = task.getFuture();
			if (future != null) {
				future.cancel(true);
			}
			//删除任务
			currentHashMap.remove(id);
		}
		return task;
	}

	/**
	 * 根据ID删除,调用暂停的方法
	 * @param id
	 * @return
	 */
	public TaskListDto deleteMap(String id){
		return cancelMap(id);
	}

	/**
	 * 重新启动
	 * @param id
	 * @return
	 */
	public TaskListDto restartMap(TaskList taskList) {
		TaskListDto taskListDto=putMap(taskList);
		return taskListDto;
	}
}
