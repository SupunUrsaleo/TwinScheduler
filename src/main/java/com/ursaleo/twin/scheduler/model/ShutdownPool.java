package com.ursaleo.twin.scheduler.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class ShutdownPool {

    @Id
    private String instanceId;  // Unique identifier for each instance

    private String status;  // Stores the status of the instance (e.g., "stopping", "stopped", etc.)

}
