package com.piesat.sod.job.job;

import java.util.Date;

public class MyRunnable implements Runnable {
	@Override
	public void run() {
		System.out.println("任务一----" + new Date());
	}
}
