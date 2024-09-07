package com.ursaleo.twin.scheduler.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Builder;
import lombok.Data;


import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Data
public class AppSession {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID sessionId;

    private String serverPublicIP;
    private String serverPrivateIP;
    private int mappedPort;
    private String twinVersionId;
    private String status;
    private String instanceID;
    private String partnerSecureData;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

}

