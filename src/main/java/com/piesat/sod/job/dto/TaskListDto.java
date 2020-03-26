package com.piesat.sod.job.dto;

import lombok.Data;

import java.util.concurrent.ScheduledFuture;

@Data
public class TaskListDto {
	private String cron;

	private Object task;

	private ScheduledFuture<?> future;
}
