package com.piesat.sod.sync.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.piesat.sod.job.dto.Log;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * ei发送
 *
 * @author cwh
 * @date 2020年 03月27日 09:36:44
 */
@Slf4j
@Component
public class EiSender {

    @Value("${ei.eiUrl}")
    public String eiUrl;

    public static String staEiUrl;

    @PostConstruct
    public void init() {
        staEiUrl = eiUrl;
    }

    public static void sendEI(String type,String title,String obj,String content,String result,String im,String comment,String node) {
        Log ei = createEI(type, title, obj, content, result, im, comment, node);
        log.info(" restful return {}", ei.toString());
        String res = RestfulUtil.Post(staEiUrl, toJsonStr(ei));
        if (res != null)
            log.info(" restful return {}", res);
        else
            log.info(" restful failed ");
    }


    private static Log createEI(String type, String title, String obj, String content, String result, String im, String comment,String node) {
        Log log = new Log();
        log.setName("EI告警信息");
        log.setMessage("EI告警信息");
        log.setType("SYSTEM.ALARM.EI");
        @SuppressWarnings("rawtypes")
        Map map = new HashMap<>();
        map.put("SYSTEM", "SOD");
        map.put("GROUP_ID", "OP_SOD_C_05");
        map.put("ORG_TIME", "");
        map.put("MSG_TYPE", "03");
        map.put("COL_TYPE", "02");
        map.put("DATA_FROM", "BABJ");
        map.put("EVENT_TYPE", type);
        map.put("EVENT_LEVEL", "03");
        map.put("EVENT_TITLE", title);
        map.put("KObject", obj);
        map.put("KEvent", content);
        map.put("KResult", result);
        map.put("KIndex", im);
        map.put("KComment", comment);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        map.put("EVENT_TIME", formatter.format(now));
        map.put("EVENT_SUGGEST", "1,0,0,1$通知系统管理员");
        map.put("EVENT_CONTROL", "0");
        map.put("EVENT_TRAG", "表结构变更或数据积压");
        map.put("EVENT_EXT1", "");
        map.put("EVENT_EXT2", node);
        log.setFields(map);
        return log;
    }

    public static String toJsonStr(Log log) {
        return JSONObject.toJSONStringWithDateFormat(log, "yyyy-MM-dd HH:mm:ss.SSS", SerializerFeature.DisableCircularReferenceDetect);
    }
}
