package com.piesat.sod.job.configuration;

import com.piesat.sod.job.bean.TaskList;
import com.piesat.sod.job.container.MapContainer;
import com.piesat.sod.job.service.DatabaseService;
import com.piesat.sod.job.service.TaskListService;
import com.piesat.sod.sync.config.DruidBuilder;
import com.piesat.sod.sync.entity.DatabaseEntity;
import com.piesat.sod.sync.entity.EleWarningEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Order(1)
@Slf4j
public class LoadTaskList implements CommandLineRunner {

    @Autowired
    private DatabaseService databaseService;
    @Autowired
    private TaskListService taskListService;

    public MapContainer mapContainer = MapContainer.getInstance();

    @Override
    public void run(String... args) throws Exception {
        log.info("系统启动后载入数据库中启动的定时任务");
        List<DatabaseEntity> databaseInfo = databaseService.getDatabaseInfo();
        for (DatabaseEntity d : databaseInfo) {
            DruidBuilder.druidConfigMap.put(d.getDatabaseId(), d);
        }

        //获取当前启动的任务
        List<TaskList> taskList = taskListService.getEleWarning();

        if (taskList == null || taskList.size() <= 0) {
            return;
        }
        log.info("LoadTaskList中的对象为 - " + mapContainer);

        for (TaskList t : taskList) {
            mapContainer.putMap(t, t.getEleWarningEntity());
        }
    }

}
