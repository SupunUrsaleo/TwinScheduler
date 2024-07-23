package com.ursaleo.twin.scheduler.repository;

import java.util.List;
import java.util.UUID;
import com.ursaleo.twin.scheduler.model.AppSession;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppSessionRepository extends JpaRepository<AppSession, UUID> {

    List<AppSession> findByTwinVersionIdAndStatus(String twinVersionId, String status);
}

