package com.em.pojo;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Getter
@NoArgsConstructor
public class Result implements Serializable {
    // Getter和Setter方法
    private long time;
    private Long userId;
    private Integer ruleId;

    public Result(long time, Long userId, Integer ruleId) {
        this.time = time;
        this.userId = userId;
        this.ruleId = ruleId;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    // toString方法用于打印和调试
    @Override
    public String toString() {
        return "Result{" +
                "time=" + time +
                ", userId=" + userId +
                ", ruleId='" + ruleId + '\'' +
                '}';
    }
}
