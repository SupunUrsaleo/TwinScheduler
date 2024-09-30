package com.ursaleo.twin.scheduler.service;


import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.model.ShutdownPool;
import com.ursaleo.twin.scheduler.model.TwinAvailability;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import com.ursaleo.twin.scheduler.repository.ShutdownPoolRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class SchedulerService {

    @Autowired
    private TwinAvailabilityRepository twinAvailabilityRepository;

    @Autowired
    AppSessionRepository appSessionRepository;

    @Autowired
    private ShutdownPoolRepository shutdownPoolRepository;

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

    @Value("${scheduler.autostart.minShutdownInstances}")
    private int autoStartMinShutdownInstances;

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
            
            List<ShutdownPool> stoppedInstances = shutdownPoolRepository.findByStatus(Status.STOPPED);
            // List<ShutdownPool> stoppingInstances = shutdownPoolRepository.findByStatus(Status.STOPPING);
            // // List<AppSession> stoppedInstances = appSessionRepository.findByStatus(Status.STOPPED);
            
            aliveInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            startingInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            busyInstances.stream()
                    .map(AppSession::getInstanceID)
                    .forEach(instancesForCheck::put);

            JSONArray StoppedInstanceIds = new JSONArray();
            stoppedInstances.stream()
                    .map(ShutdownPool::getInstanceId)
                    .forEach(StoppedInstanceIds::put);            
            // StoppedInstanceIds.put("i-0dad76253f0519f57");
            // StoppedInstanceIds.put("i-02177d6722c43485e");

            int nonBusyInstances = aliveInstances.size() + startingInstances.size();
            int instancesToStart = twinAvailability.getMinAvailable() - nonBusyInstances; // Calculate how many instances need to be started

            if ((nonBusyInstances < twinAvailability.getMinAvailable()) && (busyInstances.size() < twinAvailability.getMaxBusy())) {
                try {
                    // Limit the number of instances to start based on the calculated value `instancesToStart`
                    JSONArray instancesToStartArray = new JSONArray();

                    // Check if there are enough stopped instances to start
                    int maxInstances = Math.min(instancesToStart, StoppedInstanceIds.length()); // Ensure we don't exceed the number of stopped instances

                    // Add only the required number of instance IDs from the StoppedInstanceIds list
                    for (int i = 0; i < maxInstances; i++) {
                        instancesToStartArray.put(StoppedInstanceIds.get(i));
                    }

                    log.info("Starting {} instances from StoppedInstanceIds", instancesToStartArray.length());

                    // Start the selected instances
                    JSONArray startedInstances = twinHandlerService.startEC2Instances(instancesToStartArray);

                    // Loop through the started instances and log their status
                    for (int i = 0; i < startedInstances.length(); i++) {
                        JSONObject instance = startedInstances.getJSONObject(i);
                        log.info("Started EC2 instance: instanceId={}, status={}", instance.getString("instanceId"), instance.getString("status"));
                        instancesForCheck.put(instance.getString("instanceId"));
                    }

                } catch (JSONException e) {
                    log.error("Error starting instances for twin version {}. \n {}", twinAvailability.getTwinVersionId(), e.getMessage());
                }
            }

            // Invoke the twin health check with the merged instancesForCheckStopped
            twinHandlerService.invokeTwinHealthCheck(instancesForCheck, twinAvailability.getTwinVersionId());   
            
            log.info("Twin Autoscaling via {} Twin complete.", twinAvailability.getTwinVersionId());

        });

        JSONArray instancesForCheckStopped = new JSONArray();

        List<ShutdownPool> stoppedInstances = shutdownPoolRepository.findByStatus(Status.STOPPED);
        List<ShutdownPool> stoppingInstances = shutdownPoolRepository.findByStatus(Status.STOPPING);
        List<ShutdownPool> startedInstances = shutdownPoolRepository.findByStatus(Status.STARTED);

        stoppedInstances.stream()
        .map(ShutdownPool::getInstanceId)
        .forEach(instancesForCheckStopped::put);  

        stoppingInstances.stream()
        .map(ShutdownPool::getInstanceId)
        .forEach(instancesForCheckStopped::put);

        startedInstances.stream()
        .map(ShutdownPool::getInstanceId)
        .forEach(instancesForCheckStopped::put);

        JSONArray instanceIds = new JSONArray();
        if (stoppedInstances.size() + stoppingInstances.size() < autoStartMinShutdownInstances) {
            try {
                // Start new instances and get the JSONArray of newly started instance IDs //autoStartMinShutdownInstances = twinAvailability.getMinReserved()
                JSONArray newInstances = twinHandlerService.startInstances(autoStartMinShutdownInstances - stoppedInstances.size() - stoppingInstances.size());
                
                ExecutorService executorService = Executors.newFixedThreadPool(newInstances.length());
        
                // Add each new instance ID from the JSONArray to instancesForCheckStopped
                for (int i = 0; i < newInstances.length(); i++) {
                    String instanceId = newInstances.getString(i);
                    instanceIds.put(instanceId);
                    log.info("Created a new Stopped instance {}", instanceId);
        
                    // Run the state check and stop operation asynchronously
                    CompletableFuture.runAsync(() -> {
                        try {
                            boolean isRunning = false;
                            while (!isRunning) {
                                // Check the current state of the instance
                                String instanceState = twinHandlerService.checkInstanceState(instanceId);  // Implement a method to get the state of the instance
                                
                                log.info("Current state of instance {}: {}", instanceId, instanceState);
        
                                if ("running".equals(instanceState)) {
                                    log.info("Instance {} is now running.Forced to Stop it.", instanceId);
                                    
                                    // Stop the instance
                                    twinHandlerService.stopEC2Instances(instanceIds);  // Implement the method to stop the instance
                                    
                                    instancesForCheckStopped.put(instanceId);
                                    
                                    // Invoke the twin stop check with the merged instancesForCheckStopped
                                    twinHandlerService.invokeTwinStoppedCheck(instancesForCheckStopped);  

                                    log.info("Instance {} has been stopped.", instanceId);
                                    isRunning = true;
                                } else {
                                    // If not yet running, wait for some time before checking again
                                    Thread.sleep(5000);  // Wait 5 seconds before the next check
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.error("Thread was interrupted during the stop process for instance {}", instanceId);
                        } catch (Exception e) {
                            log.error("Error in asynchronous stop operation for instance {}. \n{}", instanceId, e.getMessage());
                        }
                    }, executorService);
                }
            } catch (JSONException e) {
                log.error("Error starting instances from shutdown Pool. \n {}", e.getMessage());
            }
        }                  

        // Invoke the twin stop check with the merged instancesForCheckStopped
        try {
            twinHandlerService.invokeTwinStoppedCheck(instancesForCheckStopped);
        } catch (JSONException e) {
            log.error("Error invoking twin stopped check: {}", e.getMessage());
        }

    log.info("Twin Autoscaling complete.");
    
    }
}
