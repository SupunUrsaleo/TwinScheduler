package com.ursaleo.twin.scheduler.repository;

import com.ursaleo.twin.scheduler.model.ContainerAvailability;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface ContainerAvailabilityRepository extends JpaRepository<ContainerAvailability, String> {

    // Get total available containers
    @Query("SELECT COALESCE(SUM(c.availableContainers), 0) FROM ContainerAvailability c")
    int getTotalAvailableContainers();

    // Get instances with available containers
    @Query("SELECT c FROM ContainerAvailability c WHERE c.availableContainers > 0 ORDER BY c.instanceId ASC")
    List<ContainerAvailability> findByAvailableContainersGreaterThan(int minAvailable);

    // Get a container record by instanceId
    @Query("SELECT c FROM ContainerAvailability c WHERE c.instanceId = :instanceId")
    ContainerAvailability findByInstanceId(String instanceId);

}
