package com.ursaleo.twin.scheduler.repository;

import com.ursaleo.twin.scheduler.model.TwinAvailability;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface TwinAvailabilityRepository extends JpaRepository<TwinAvailability, String> {

    TwinAvailability findByTwinVersionId(String twinVersionId);

    @Query("SELECT COALESCE(SUM(t.minAvailable), 0) FROM TwinAvailability t")
    int getTotalMinAvailable();
}
