package com.em.pojo;

import lombok.Getter;

@Getter
public enum EventTypeEnum {
    VIEW(1, "View"),
    PURCHASE(2, "Purchase");

    private final int type;
    private final String description;

    EventTypeEnum(int type, String description) {
        this.type = type;
        this.description = description;
    }

    public int getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }
}
