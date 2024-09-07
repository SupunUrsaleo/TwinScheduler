package com.ursaleo.twin.scheduler.repository;

import com.ursaleo.twin.scheduler.model.TwinAvailability;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TwinAvailabilityRepository extends JpaRepository<TwinAvailability, String> {

    TwinAvailability findByTwinVersionId(String twinVersionId);
}
