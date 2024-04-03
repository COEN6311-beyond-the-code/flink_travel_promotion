package com.em.pojo;

public class Result {
    private long time;
    private Long userId;
    private Integer ruleId;

    public Result(long time, Long userId, Integer ruleId) {
        this.time = time;
        this.userId = userId;
        this.ruleId = ruleId;
    }

    // Getter和Setter方法
    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Integer getRuleId() {
        return ruleId;
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
