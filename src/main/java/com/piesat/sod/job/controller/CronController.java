package com.piesat.sod.job.controller;

import com.piesat.sod.job.bean.TaskList;
import com.piesat.sod.job.service.CronService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@Api("定时器动态处理的控制器")
@RestController
public class CronController {

	@Autowired
	private CronService cronService;

	@ApiOperation("增加定时任务")
	@ApiImplicitParam(name="taskList",value="定时任务属性",required=true,dataTypeClass= TaskList.class)
	@GetMapping("/addCron")
	public String addCron(TaskList taskList){
		cronService.addTaskList(taskList);
		return "success";
	}

	@ApiOperation("修改定时任务")
	@ApiImplicitParam(name="taskList",value="定时任务属性",required=true,dataTypeClass=TaskList.class)
	@GetMapping("/updateCron")
	public String updateCron(TaskList taskList){
		cronService.updateTaskList(taskList.getId(),taskList.getCron());
		return "success";
	}

	@ApiOperation("暂停定时任务")
	@GetMapping("/stopCronByid")
	@ApiImplicitParam(name="id",value="定时任务主键id",required=true,dataTypeClass=String.class)
	public String stopCron(String id) {
		cronService.cancelTaskList(id);
		return "success";
	}

	@ApiOperation("删除定时任务")
	@GetMapping("/deleteCronByid")
	@ApiImplicitParam(name="id",value="定时任务主键id",required=true,dataTypeClass=String.class)
	public String deleteCron(String id) {
		cronService.deleteTaskList(id);
		return "success";
	}

	@ApiOperation("重新启动定时任务")
	@GetMapping("/restartCronByid")
	@ApiImplicitParam(name="id",value="定时任务主键id",required=true,dataTypeClass=String.class)
	public String restartCron(String id) {
		cronService.restartTaskList(id);
		return "success";
	}

	@ApiOperation("获取所有job包下类的全类名")
	@GetMapping("/getJobClass")
	public List<String> getJobClass(){
		List<Class<?>> list=cronService.getJobClass("com.plf.task.scheduled.job");
		List<String> listString = list.stream().map(c -> c.toString()).collect(Collectors.toList());
		return listString;
	}


}
