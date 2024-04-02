package com.em.pojo;

import java.io.Serializable;

public class Event implements Serializable {
    public Long timestamp;
    public String category;
    public Integer itemId;
    public Long userId;


    public Event(Long timestamp, String category, Integer itemId, Long userId) {
        this.timestamp = timestamp;
        this.category = category;
        this.itemId = itemId;
        this.userId = userId;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", category='" + category + '\'' +
                ", itemId='" + itemId + '\'' +
                ", userId='" + userId + '\'' +
                '}';
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
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

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }
}