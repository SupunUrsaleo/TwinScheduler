package com.ursaleo.twin.scheduler.service;


import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.model.TwinAvailability;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Service
@Slf4j
public class SchedulerService {

    @Autowired
    private TwinAvailabilityRepository twinAvailabilityRepository;

    @Autowired
    AppSessionRepository appSessionRepository;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Autowired
    TwinHandlerService twinHandlerService;

    @Value("${endpoints.lambda.healthcheck}")
    private String healthCheckEndpoint;

    @Value("${scheduler.healthcheck.fixedRate}")
    private long healthCheckRate;

    @Value("${scheduler.autoscale.fixedRate}")
    private long autoScaleRate;

    @Value("${scheduler.autoscale.minInstances}")
    private long autoScaleMinInstances;

    @PostConstruct
    public void scheduleTasks() {
        log.info("Starting Scheduled Tasks.");
        scheduledExecutorService.scheduleAtFixedRate(this::performAutoScaling, 0, autoScaleRate, TimeUnit.MILLISECONDS);
        log.info("Scheduled Tasks Completed.");
    }

    public void performAutoScaling() {

        log.info("Starting Twin Autoscaler..");
        List<TwinAvailability> allTwins = twinAvailabilityRepository.findAll();

        allTwins.forEach(twinAvailability -> {
            JSONArray instancesForCheck = new JSONArray();

            List<AppSession> aliveInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.AVAILABLE);
            List<AppSession> busyInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.BUSY);
            List<AppSession> startingInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.STARTING);

            aliveInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            startingInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            busyInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            // Check if there are fewer alive and starting instances than the minimum required
            int nonBusyInstances = aliveInstances.size() + startingInstances.size();
            if ( (nonBusyInstances < twinAvailability.getMinAvailable()) && (busyInstances.size() < twinAvailability.getMaxBusy())) {
                try {
                    // Start new instances and get the JSONArray of newly started instance IDs
                    JSONArray newInstances = twinHandlerService.startInstances(twinAvailability.getMinAvailable() - aliveInstances.size());

                    // Add each new instance ID from the JSONArray to instancesForCheck
                    for (int i = 0; i < newInstances.length(); i++) {
                        instancesForCheck.put(newInstances.get(i));
                    }

                } catch (JSONException e) {
                    log.error("Error starting instances for twin version {}. \n {}", twinAvailability.getTwinVersionId(), e.getMessage());
                }
            }

            // Invoke the twin health check with the merged instancesForCheck
            twinHandlerService.invokeTwinHealthCheck(instancesForCheck, twinAvailability.getTwinVersionId());
        });

        log.info("Twin Autoscaling complete.");
    }


}
