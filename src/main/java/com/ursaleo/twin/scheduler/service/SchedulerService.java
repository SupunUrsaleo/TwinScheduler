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
//TODO: how to handle different twin instances? using different AMI or changing a config.
        allTwins.forEach(twinAvailability -> {
            JSONArray instancesForCheck = new JSONArray();
        List<AppSession> aliveInstances = appSessionRepository.findByTwinVersionIdAndStatusIn(twinAvailability.getTwinVersionId(), Arrays.asList(Status.AVAILABLE, Status.BUSY)); //TODO: how to handle dead instances?
            //TODO: DO i need to health check busy instances. currently checking?
            List<String> instanceIds = aliveInstances.stream()
                        .map(AppSession::getInstanceID)
                        .toList();

                if(instanceIds.size() < twinAvailability.getMinAvailable()){
                    try {
                        instancesForCheck = twinHandlerService.startInstances(twinAvailability.getMinAvailable() - instanceIds.size());
                    } catch (JSONException e) {
                        log.error("Error Starting instances for twin version {}. \n {}",twinAvailability.getTwinVersionId(),e.getMessage());
                    }
                }else{
                    instancesForCheck = new JSONArray(
                            aliveInstances.stream()
                                    .map(AppSession::getInstanceID)
                                    .toList()
                    );
                }

                twinHandlerService.invokeTwinHealthCheck(instancesForCheck, twinAvailability.getTwinVersionId());

            });

        log.info("Twin Autoscaling complete.");
    }


}
