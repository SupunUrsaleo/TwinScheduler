package com.ursaleo.twin.scheduler.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class TwinAvailability {

    @Id
    private String twinVersionId;
    private int minAvailable;
    private int maxBusy;

}

