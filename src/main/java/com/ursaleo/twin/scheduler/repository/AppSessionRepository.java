package com.ursaleo.twin.scheduler.repository;

import java.util.List;
import java.util.UUID;
import com.ursaleo.twin.scheduler.model.AppSession;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface AppSessionRepository extends JpaRepository<AppSession, UUID> {

    List<AppSession> findByTwinVersionIdAndStatus(String twinVersionId, String status);

    List<AppSession> findByTwinVersionIdAndStatusIn(String twinVersionId, List<String> statuses);
    List<AppSession> findByStatus(String status);

    AppSession findByInstanceID(String instanceID);
    AppSession findByContainerID(String containerID);

    AppSession findByServerPublicIP(String serverPublicIP);
    AppSession findByServerPublicIPAndMappedPort(String serverPublicIP, int mappedPort);

    

    // Count available machines (replace "AVAILABLE" with the correct status)
    @Query("SELECT COUNT(a) FROM AppSession a WHERE a.status = 'Available'")
    int countAvailableMachines();

    @Query("SELECT COUNT(a) FROM AppSession a WHERE a.status ='Starting'")
    int countStartingMachines();
}

