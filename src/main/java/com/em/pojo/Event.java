package com.em.pojo;

import java.io.Serializable;

public class Event implements Serializable {
    public Long timestamp;
    public String category;
    public String itemId;
    public String userId;

    public Event(long timestamp, String category, String itemId, String userId) {
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

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}