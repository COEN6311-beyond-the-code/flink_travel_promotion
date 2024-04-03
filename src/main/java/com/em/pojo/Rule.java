package com.em.pojo;

import java.io.Serializable;

public class Rule implements Serializable {
    public Integer id;
    public String category;
    public Integer itemId;
    public Integer browseTimes;
    public Integer windowsTime;
    public Integer waitTime;


    public Rule() {
    }


    public Rule(Integer id, String category, Integer itemId, Integer browseTimes, Integer windowsTime, Integer waitTime) {
        this.id = id;
        this.category = category;
        this.itemId = itemId;
        this.browseTimes = browseTimes;
        this.windowsTime = windowsTime;
        this.waitTime = waitTime;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Integer getItemId() {
        return itemId;
    }

    public void setItemId(Integer itemId) {
        this.itemId = itemId;
    }

    public Integer getBrowseTimes() {
        return browseTimes;
    }

    public void setBrowseTimes(Integer browseTimes) {
        this.browseTimes = browseTimes;
    }

    public Integer getWindowsTime() {
        return windowsTime;
    }

    public void setWindowsTime(Integer windowsTime) {
        this.windowsTime = windowsTime;
    }

    public Integer getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(Integer waitTime) {
        this.waitTime = waitTime;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id=" + id +
                ", category='" + category + '\'' +
                ", itemId=" + itemId +
                ", browseTimes=" + browseTimes +
                ", windowsTime=" + windowsTime +
                ", waitTime=" + waitTime +
                '}';
    }
}
