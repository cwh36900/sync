package com.piesat.sod.job.bean;

import com.piesat.sod.sync.entity.EleWarningEntity;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;

/**
 * 任务表
 * @author plf 2019年5月5日上午11:19:57
 *
 */
@Data
@ApiModel("定时任务")
public class TaskList {

	@ApiModelProperty(value="主键ID-",required=false,example="1")
	private String id;

	/**
	 * 任务表达式
	 */
	@ApiModelProperty(value="任务表达式",required=true,example="*/5 * * * * ?")
	private String cron;

	/**
	 * 任务全类名
	 */
	@ApiModelProperty(value="任务全类名",required=true,example="com.plf.task.scheduled.job.MyRunnable")
	private String clazz;

	/**
	 * 状态
	 * 0 代表 删除
	 * 1 代表 启动
	 * 2 代表 停止
	 */
	@ApiModelProperty(value="任务状态 0 删除 1启动 2 停止",required=false,example="0")
	private Integer status;

	/**
	 * 任务名
	 */
	@ApiModelProperty(value="任务名",required=true,example="任务一")
	private String taskname;

	/**
	 * 创建时间
	 */
	@ApiModelProperty(value="创建时间",required=false,example="2019-05-07 12:00:00")
	private Date createtime;

	private EleWarningEntity eleWarningEntity;

}
