package com.ursaleo.twin.scheduler.repository;

import com.ursaleo.twin.scheduler.model.ShutdownPool;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ShutdownPoolRepository extends JpaRepository<ShutdownPool, String> {

    // Custom query method to find instances by their status
    List<ShutdownPool> findByStatus(String status);

    ShutdownPool findByInstanceId(String instanceId);

}
