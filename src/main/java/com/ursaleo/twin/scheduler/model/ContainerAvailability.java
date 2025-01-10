package com.ursaleo.twin.scheduler.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class ContainerAvailability {

    @Id
    private String instanceId;
    private int maxContainers;
    private int availableContainers;
    // private int minReserved;
}

