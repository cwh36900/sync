package com.piesat.sod.job.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class Log {
    @JsonProperty("type")
    private String type = null;
    @JsonProperty("name")
    private String name = null;
    @JsonProperty("message")
    private String message = null;
    @JsonProperty("occur_time")
    private Long occur_time = null;
    @JsonProperty("receive_time")
    private Long receiveTime = null;
    @JsonProperty("fields")
    private Map<String, Object> fields = new LinkedHashMap<>();




    public Log() {
    }
//    @ApiModelProperty(
//            example = "TopologyChangedLog",
//            required = true,
//            value = "日志类型标识"
//    )

    public String getType() {
        return this.type;
    }
    public void setType(String type) {
        this.type = type;
    }
//    public Log name(String name) {
//        this.name = name;
//        return this;
//    }
//    @ApiModelProperty(
//            example = "拓扑变更",
//            value = "简短的日志描述"
//    )
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Log message(String message) {
        this.message = message;
        return this;
    }

    @ApiModelProperty(
            example = "在设备192.168.0.1:eth0/1端口上连接了新的设备192.168.0.2:eth0/3",
            required = true,
            value = "详细的日志信息描述"
    )
    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

//    public Log occur_time(Long occurTime) {
//        this.occur_time = occurTime;
//        return this;
//    }

    @ApiModelProperty(
            example = "1487642401966",
            value = "日志产生时间，如果不提供则为服务端接收到的时间"
    )
    public Long getOccur_time() {
        return this.occur_time;
    }

    public void setOccur_time(Long occurTime) {
        this.occur_time = occurTime;
    }

    public Log receiveTime(Long receiveTime) {
        this.receiveTime = receiveTime;
        return this;
    }

    @ApiModelProperty(
            example = "1487642401966",
            value = "日志接收时间，如果不提供则为服务端接收到的时间"
    )
    public Long getReceiveTime() {
        return this.receiveTime;
    }

    public void setReceiveTime(Long receiveTime) {
        this.receiveTime = receiveTime;
    }

    public Log fields(Map<String, Object> fields) {
        this.fields = fields;
        return this;
    }

    public Log putFieldsItem(String key, Object fieldsItem) {
        this.fields.put(key, fieldsItem);
        return this;
    }

    @ApiModelProperty("扩展属性集合")
    public Map<String, Object> getFields() {
        return this.fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(o != null && this.getClass() == o.getClass()) {
            Log log = (Log)o;
            return Objects.equals(this.type, log.type) && Objects.equals(this.name, log.name) && Objects.equals(this.message, log.message) && Objects.equals(this.occur_time, log.occur_time) && Objects.equals(this.receiveTime, log.receiveTime) && Objects.equals(this.fields, log.fields);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.type, this.name, this.message, this.occur_time, this.receiveTime, this.fields});
    }

//    public String toString() {
//        StringBuilder sb = new StringBuilder();
//        sb.append("class Log {\n");
//        sb.append("    type: ").append(this.toIndentedString(this.type)).append("\n");
//        sb.append("    name: ").append(this.toIndentedString(this.name)).append("\n");
//        sb.append("    message: ").append(this.toIndentedString(this.message)).append("\n");
//        sb.append("    occur_time: ").append(this.toIndentedString(this.occur_time)).append("\n");
//        sb.append("    receiveTime: ").append(this.toIndentedString(this.receiveTime)).append("\n");
//        sb.append("    fields: ").append(this.toIndentedString(this.fields)).append("\n");
//        sb.append("}");
//        return sb.toString();
//    }

//    private String toIndentedString(Object o) {
//        return o == null?"null":o.toString().replace("\n", "\n    ");
//    }
}
