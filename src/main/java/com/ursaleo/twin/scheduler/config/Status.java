package com.ursaleo.twin.scheduler.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Status {

    public static final String STARTING = "Starting";
    public static final String AVAILABLE = "Available";
    public static final String BUSY = "Busy";
    public static final String DEAD = "Dead";
}