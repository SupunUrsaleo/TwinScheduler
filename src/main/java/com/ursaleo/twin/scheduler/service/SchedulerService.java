package com.ursaleo.twin.scheduler.service;


import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.model.ShutdownPool;
import com.ursaleo.twin.scheduler.model.TwinAvailability;
import com.ursaleo.twin.scheduler.model.ContainerAvailability;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import com.ursaleo.twin.scheduler.repository.ShutdownPoolRepository;
import com.ursaleo.twin.scheduler.repository.ContainerAvailabilityRepository;
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

import java.util.Set;
import java.util.HashSet;


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
    private ContainerAvailabilityRepository containerAvailabilityRepository;

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

    @Value("${scheduler.autoscale.minContainers}")
    private int autoScaleMinContainers;

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

        int totalMinAvailable = twinAvailabilityRepository.getTotalMinAvailable();
        log.info("Total minAvailable Sessions: {}", totalMinAvailable);

        JSONArray instancesForCheck2 = new JSONArray();

        int availableSessions = appSessionRepository.countAvailableMachines();
        log.info("Total available Sessions: {}", availableSessions);
        int startingSessions = appSessionRepository.countStartingMachines();
        log.info("Total starting Sessions: {}", startingSessions);

        int totalAvailableContainers = containerAvailabilityRepository.getTotalAvailableContainers();
        log.info("Total available Containers: {}", totalAvailableContainers);

        List<ShutdownPool> stoppedInstances2 = shutdownPoolRepository.findByStatus(Status.STOPPED);

        JSONArray StoppedInstanceIds2 = new JSONArray();
        stoppedInstances2.stream()
                .map(ShutdownPool::getInstanceId)
                .forEach(StoppedInstanceIds2::put);   
        
        try {
            // Calculate how many more containers are needed
            int containerDeficit = totalMinAvailable - totalAvailableContainers - availableSessions - startingSessions;

            JSONArray instancesToStartArray = new JSONArray();

            if (containerDeficit > 0) {
                // Calculate how many instances to start to meet the deficit
                int instancesToStart = (containerDeficit % autoScaleMinContainers == 0) ? (containerDeficit / autoScaleMinContainers) : (containerDeficit / autoScaleMinContainers + 1);
                log.info("Total instances to start: {}", instancesToStart);

                if (instancesToStart > 0) {
                    // Check if there are enough stopped instances to start
                    int maxInstances = Math.min(instancesToStart, StoppedInstanceIds2.length()); // Ensure we don't exceed the number of stopped instances

                    // Add only the required number of instance IDs from the StoppedInstanceIds list
                    for (int i = 0; i < maxInstances; i++) {
                        instancesToStartArray.put(StoppedInstanceIds2.get(i));
                    }

                    log.info("Starting {} instances from StoppedInstanceIds", instancesToStartArray.length());

                    if(maxInstances > 0){
                        // Start the selected instances
                        JSONArray startedInstances = twinHandlerService.startEC2Instances(instancesToStartArray);
                        

                        // Loop through the started instances and log their status
                        for (int i = 0; i < startedInstances.length(); i++) {
                            JSONObject instance = startedInstances.getJSONObject(i);
                            log.info("Started EC2 instance: instanceId={}, status={}", instance.getString("instanceId"), instance.getString("status"));
                            // instancesForCheck.put(instance.getString("instanceId"));
                            String instanceId = instance.getString("instanceId");
                            // instancesForCheck2.put(instanceId);
                            ShutdownPool shutdownPoolEntry = shutdownPoolRepository.findByInstanceId(instanceId);
                            // Update ShutdownPool status to reflect stopped state
                            shutdownPoolEntry.setStatus(Status.STARTED);
                            shutdownPoolRepository.save(shutdownPoolEntry);
            
                            // Create and save a new record in ContainerAvailability
                            ContainerAvailability containerAvailability = new ContainerAvailability();
                            containerAvailability.setInstanceId(instanceId);
                            containerAvailability.setMaxContainers(autoScaleMinContainers);  // Default max containers per instance
                            containerAvailability.setAvailableContainers(autoScaleMinContainers); // Initially, all are available
            
                            containerAvailabilityRepository.save(containerAvailability);
                            log.info("Added new ContainerAvailability entry for instance {}", instanceId);

                    }
                    }

                    if(maxInstances == 0){
                        log.info("No enough shutdown pool instances");
                        JSONArray newInstances = twinHandlerService.startInstances(instancesToStart);
                        log.info("Starting new instances. Instances Ids = {}", newInstances);
                        // Add each new instance ID from the JSONArray to instancesForCheck
                        for (int i = 0; i < newInstances.length(); i++) {
                            // instancesForCheck.put(newInstances.get(i));
                            String instanceId = newInstances.getString(i);
                            // instancesForCheck2.put(instanceId);
            
                            // Create and save a new record in ContainerAvailability
                            ContainerAvailability containerAvailability = new ContainerAvailability();
                            containerAvailability.setInstanceId(instanceId);
                            containerAvailability.setMaxContainers(autoScaleMinContainers);  // Default max containers per instance
                            containerAvailability.setAvailableContainers(autoScaleMinContainers); // Initially, all are available
            
                            containerAvailabilityRepository.save(containerAvailability);
                            log.info("Added new ContainerAvailability entry for instance {}", instanceId);                                                
                    }
                    }
            }
        }

        // Add a delay before calling invokeTwinHealthCheck
        try {
            Thread.sleep(1000);  // Adjust delay as needed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting before health check.");
            // No need to throw an exception if logging is sufficient
        }

        // Invoke the twin stop check with the merged instancesForCheckStopped
        try {
            twinHandlerService.invokeTwinStoppedCheck(StoppedInstanceIds2);
            log.info("Shutdown Pool checked after initial instance check.");
        } catch (JSONException e) {
            log.error("Error invoking twin stopped check: {}", e.getMessage());
        }
        
        
                // if (instancesToStart2 > 0) {
                    
                //     // Start new instances
                //     JSONArray newInstances2 = twinHandlerService.startInstances(instancesToStart2);
                //     log.info("Starting new instances. Instances Ids = {}", newInstances2);
                    
                //     // Add each new instance ID from the JSONArray to instancesForCheck
                //     for (int i = 0; i < newInstances2.length(); i++) {
                //         String instanceId = newInstances2.getString(i);
                //         instancesForCheck2.put(instanceId);
        
                //         // Create and save a new record in ContainerAvailability
                //         ContainerAvailability containerAvailability = new ContainerAvailability();
                //         containerAvailability.setInstanceId(instanceId);
                //         containerAvailability.setMaxContainers(autoScaleMinContainers);  // Default max containers per instance
                //         containerAvailability.setAvailableContainers(autoScaleMinContainers); // Initially, all are available
        
                //         containerAvailabilityRepository.save(containerAvailability);
                //         log.info("Added new ContainerAvailability entry for instance {}", instanceId);
                //     }
                //     log.info("Starting new instances. Instances Ids = {}", newInstances2);
                //     // Add a delay before calling invokeTwinHealthCheck
                //     try {
                //         Thread.sleep(1000);  // Adjust delay as needed
                //     } catch (InterruptedException e) {
                //         Thread.currentThread().interrupt();
                //         log.error("Interrupted while waiting before health check.");
                //     }
        
                //     // Invoke the twin health check
                //     // twinHandlerService.invokeTwinHealthCheck(instancesForCheck2, twinAvailability.getTwinVersionId()); 
                // }

        }

        catch (Exception e) {
            log.error("Error while starting instances: ", e);
        }
        
        allTwins.forEach(twinAvailability -> {
            JSONArray instancesForCheck = new JSONArray();
            

            List<AppSession> aliveInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.AVAILABLE);
            List<AppSession> busyInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.BUSY);
            List<AppSession> startingInstances = appSessionRepository.findByTwinVersionIdAndStatus(twinAvailability.getTwinVersionId(), Status.STARTING);
            
            List<ShutdownPool> stoppedInstances = shutdownPoolRepository.findByStatus(Status.STOPPED);
            // List<ShutdownPool> stoppingInstances = shutdownPoolRepository.findByStatus(Status.STOPPING);
            // // List<AppSession> stoppedInstances = appSessionRepository.findByStatus(Status.STOPPED);
            
            aliveInstances.stream()
                    .map(AppSession::getContainerID)
                    .forEach(instancesForCheck::put);

            startingInstances.stream()
                    .map(AppSession::getContainerID)
                    .forEach(instancesForCheck::put);

            busyInstances.stream()
                    .map(AppSession::getContainerID)
                    .forEach(instancesForCheck::put);

            JSONArray StoppedInstanceIds = new JSONArray();
            stoppedInstances.stream()
                    .map(ShutdownPool::getInstanceId)
                    .forEach(StoppedInstanceIds::put);            
            // StoppedInstanceIds.put("i-0dad76253f0519f57");
            // StoppedInstanceIds.put("i-02177d6722c43485e");

            int nonBusyInstances = aliveInstances.size() + startingInstances.size();
            // int instancesToStart = twinAvailability.getMinAvailable() - nonBusyInstances; // Calculate how many instances need to be started

            /* 
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

                    if(maxInstances > 0){
                    // Start the selected instances
                    JSONArray startedInstances = twinHandlerService.startEC2Instances(instancesToStartArray);
                    

                    // Loop through the started instances and log their status
                    for (int i = 0; i < startedInstances.length(); i++) {
                        JSONObject instance = startedInstances.getJSONObject(i);
                        log.info("Started EC2 instance: instanceId={}, status={}", instance.getString("instanceId"), instance.getString("status"));
                        instancesForCheck.put(instance.getString("instanceId"));
                    }
                    }

                    if(maxInstances == 0){
                    log.info("No enough shutdown pool instances");
                    JSONArray newInstances = twinHandlerService.startInstances(instancesToStart);
                    log.info("Starting new instances. Instances Ids = {}", newInstances);
                    // Add each new instance ID from the JSONArray to instancesForCheck
                    for (int i = 0; i < newInstances.length(); i++) {
                        instancesForCheck.put(newInstances.get(i));
                    }
                    }            

                } catch (JSONException e) {
                    log.error("Error starting instances for twin version {}. \n {}", twinAvailability.getTwinVersionId(), e.getMessage());
                }
            }
*/

            if ((nonBusyInstances < twinAvailability.getMinAvailable()) && (busyInstances.size() < twinAvailability.getMaxBusy())) {
                try {
                    JSONArray containersToStartArray = new JSONArray();
                    List<ContainerAvailability> availableContainers = containerAvailabilityRepository.findByAvailableContainersGreaterThan(0);

                    int containersNeeded = twinAvailability.getMinAvailable() - nonBusyInstances;
                    log.info("Total containers needed: {}", containersNeeded);

                    // Assign existing available containers first
                    for (ContainerAvailability container : availableContainers) {
                        if (containersNeeded == 0) break; // Stop if enough containers are assigned

                        int available = container.getAvailableContainers();
                        String instanceId = container.getInstanceId();

                        // Get active containers for this instance
                        List<String> activeContainers = appSessionRepository.findActiveContainersByInstanceId(instanceId);

                        // Extract existing container numbers (only 1, 2, autoScaleMinContainers)
                        Set<Integer> assignedNumbers = new HashSet<>();
                        for (String activeContainer : activeContainers) {
                            String[] parts = activeContainer.split("_");
                            if (parts.length == 2) {
                                try {
                                    int num = Integer.parseInt(parts[1]);
                                    if (num >= 1 && num <= autoScaleMinContainers) { // Only track 1, 2, or autoScaleMinContainers
                                        assignedNumbers.add(num);
                                    }
                                } catch (NumberFormatException ignored) {}
                            }
                        }

                        for (int i = 0; i < available; i++) { // Loop through available containers
                            if (containersNeeded == 0) break;

                            // Find the lowest available number within [1, 2, autoScaleMinContainers]
                            int containerNumber = 1;
                            while (assignedNumbers.contains(containerNumber) && containerNumber <= autoScaleMinContainers) {
                                containerNumber++;
                            }

                            if (containerNumber > autoScaleMinContainers) {
                                log.warn("No available container slots for instance {}", instanceId);
                                break; // Stop if all slots are occupied
                            }

                            assignedNumbers.add(containerNumber); // Mark as assigned

                            String containerId = instanceId + "_" + containerNumber;
                            log.info("Assigning container {} from instance {}", containerId, instanceId);
                            containersToStartArray.put(containerId);

                            // Update container availability
                            container.setAvailableContainers(container.getAvailableContainers() - 1);
                            containerAvailabilityRepository.save(container);

                            containersNeeded--;
                        }
                    }


                    // If more containers are needed, launch new ones
                    if (containersNeeded > 0) {
                        log.info("Not enough available containers, launching new ones.");
                        JSONArray instancesToStartArray = new JSONArray();

                        int instancesToStart = (containersNeeded % autoScaleMinContainers == 0) ? (containersNeeded / autoScaleMinContainers) : (containersNeeded / autoScaleMinContainers + 1);
                        // JSONArray newInstances = twinHandlerService.startInstances(instancesToStart);

                        if (instancesToStart > 0) {
                            // Check if there are enough stopped instances to start
                            int maxInstances = Math.min(instancesToStart, StoppedInstanceIds.length()); // Ensure we don't exceed the number of stopped instances

                            // Add only the required number of instance IDs from the StoppedInstanceIds list
                            for (int i = 0; i < maxInstances; i++) {
                                instancesToStartArray.put(StoppedInstanceIds.get(i));
                            }

                            log.info("Starting {} instances from StoppedInstanceIds", instancesToStartArray.length());

                            if(maxInstances > 0){
                                // Start the selected instances
                                JSONArray startedInstances = twinHandlerService.startEC2Instances(instancesToStartArray);
                                
                                // Loop through the started instances and log their status
                                for (int i = 0; i < startedInstances.length(); i++) {
                                    JSONObject instance = startedInstances.getJSONObject(i);
                                    log.info("Started EC2 instance: instanceId={}, status={}", instance.getString("instanceId"), instance.getString("status"));

                                    String instanceId = instance.getString("instanceId");

                                    ShutdownPool shutdownPoolEntry = shutdownPoolRepository.findByInstanceId(instanceId);
                                    // Update ShutdownPool status to reflect stopped state
                                    shutdownPoolEntry.setStatus(Status.STARTED);
                                    shutdownPoolRepository.save(shutdownPoolEntry);

                                    // Create and save a new record in ContainerAvailability
                                    ContainerAvailability containerAvailability = new ContainerAvailability();
                                    containerAvailability.setInstanceId(instanceId);
                                    containerAvailability.setMaxContainers(autoScaleMinContainers);
                                    containerAvailability.setAvailableContainers(autoScaleMinContainers);
                                    // containerAvailabilityRepository.save(containerAvailability); // Save once at the beginning

                                    int setavailableContainers = autoScaleMinContainers; // Start with max available containers

                                    for (int j = 0; j < autoScaleMinContainers; j++) { // Each instance gets autoScaleMinContainers containers
                                        if (containersNeeded == 0) break;

                                        int containerNumber = j + 1;
                                        String containerId = instanceId + "_" + containerNumber;
                                        containersToStartArray.put(containerId);
                                        log.info("Adding new container {} from instance {}", containerId, instanceId);

                                        setavailableContainers--; // Reduce available containers count
                                        containersNeeded--;
                                    }

                                    // Update available containers once after the loop
                                    containerAvailability.setAvailableContainers(setavailableContainers);
                                    containerAvailabilityRepository.save(containerAvailability); // Save only once after updating
                                }
                            }

                            if(maxInstances == 0){
                                log.info("No enough shutdown pool instances");
                                JSONArray newInstances = twinHandlerService.startInstances(instancesToStart);
                                log.info("Starting new instances. Instances Ids = {}", newInstances);
                                // Add each new instance ID from the JSONArray to instancesForCheck
                                for (int i = 0; i < newInstances.length(); i++) {
                                    // instancesForCheck.put(newInstances.get(i));
                                    String instanceId = newInstances.getString(i);
                                    // instancesForCheck2.put(instanceId);
                    
                                    // Create and save a new record in ContainerAvailability
                                    ContainerAvailability containerAvailability = new ContainerAvailability();
                                    containerAvailability.setInstanceId(instanceId);
                                    containerAvailability.setMaxContainers(autoScaleMinContainers);  // Default max containers per instance
                                    containerAvailability.setAvailableContainers(autoScaleMinContainers); // Initially, all are available
                    
                                    int setavailableContainers = autoScaleMinContainers; // Start with max available containers
                                    // containerAvailabilityRepository.save(containerAvailability);
                                    log.info("Added new ContainerAvailability entry for instance {}", instanceId);

                                    for (int j = 0; j < autoScaleMinContainers; j++) { // Each instance gets autoScaleMinContainers containers
                                        if (containersNeeded == 0) break;

                                        int containerNumber = j + 1; // Start from 1 since it's a new instance
                                        String containerId = instanceId + "_" + containerNumber;
                                        containersToStartArray.put(containerId);
                                        log.info("Adding new container {} from instance {}", containerId, instanceId);
                                        
                                        setavailableContainers--; // Reduce available containers count
                                        containersNeeded--;
                                    }     
                                    // Update available containers once after the loop
                                    containerAvailability.setAvailableContainers(setavailableContainers);
                                    containerAvailabilityRepository.save(containerAvailability); // Save only once after updating
                                }
                            }

                        }

                        // for (int i = 0; i < newInstances.length(); i++) {
                        //     String instanceId = newInstances.getString(i);
                        //     log.info("Starting new instance: {}", instanceId);

                        //     for (int j = 0; j < autoScaleMinContainers; j++) { // Each instance gets autoScaleMinContainers containers
                        //         if (containersNeeded == 0) break;

                        //         int containerNumber = j + 1; // Start from 1 since it's a new instance
                        //         String containerId = instanceId + "_" + containerNumber;
                        //         containersToStartArray.put(containerId);
                        //         log.info("Adding new container {} from instance {}", containerId, instanceId);

                        //         ContainerAvailability newContainer = new ContainerAvailability();
                        //         newContainer.setInstanceId(instanceId);
                        //         newContainer.setMaxContainers(autoScaleMinContainers);
                        //         newContainer.setAvailableContainers(autoScaleMinContainers);
                        //         containerAvailabilityRepository.save(newContainer);

                        //         containersNeeded--;
                        //     }
                        // }
                    }

                    log.info("Starting {} containers", containersToStartArray.length());
                    twinHandlerService.invokeContainerHealthCheck(containersToStartArray, twinAvailability.getTwinVersionId());

                } catch (JSONException e) {
                    log.error("Error starting containers for twin version {}. \n {}", twinAvailability.getTwinVersionId(), e.getMessage());
                }
            }

            // Add a delay before calling invokeTwinHealthCheck
            try {
                Thread.sleep(1000);  // Adjust delay as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting before health check.");
                // No need to throw an exception if logging is sufficient
            }

            // Invoke the twin stop check with the merged instancesForCheckStopped
            try {
                twinHandlerService.invokeTwinStoppedCheck(StoppedInstanceIds);
                log.info("Shutdown Pool checked after {} Twin Autoscaling.", twinAvailability.getTwinVersionId());
            } catch (JSONException e) {
                log.error("Error invoking twin stopped check: {}", e.getMessage());
            }            

            // Invoke the twin health check with the merged instancesForCheckStopped
            twinHandlerService.invokeContainerHealthCheck(instancesForCheck, twinAvailability.getTwinVersionId());

            // twinHandlerService.invokeTwinHealthCheck(instancesForCheck, twinAvailability.getTwinVersionId()); 
            // log.info("Twin Autoscaling via {} Twin complete.", twinAvailability.getTwinVersionId());

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
        //this should be (stoppedInstances.size() + stoppingInstances.size() + startedInstances < autoStartMinShutdownInstances)
        if (stoppedInstances.size() + stoppingInstances.size() + startedInstances.size() < autoStartMinShutdownInstances) {
            try {
                // Start new instances and get the JSONArray of newly started instance IDs //autoStartMinShutdownInstances = twinAvailability.getMinReserved()
                JSONArray newInstances = twinHandlerService.startInstances(autoStartMinShutdownInstances - stoppedInstances.size() - stoppingInstances.size() - startedInstances.size());

                ExecutorService executorService = Executors.newFixedThreadPool(newInstances.length());

                // Add each new instance ID from the JSONArray to instancesForCheckStopped
                for (int i = 0; i < newInstances.length(); i++) {
                    String instanceId = newInstances.getString(i);
                    instanceIds.put(instanceId);
                    log.info("Created a new Stopped instance {}", instanceId);

                    instancesForCheckStopped.put(instanceId);

                    // Run the shutdown pool adder Lambda call asynchronously
                    CompletableFuture.runAsync(() -> {
                        try {
                            // Call the shutdown pool adder Lambda for this instance
                            JSONArray response = twinHandlerService.ShutdownPoolAdder(new JSONArray().put(instanceId));
                            log.info("Response from Shutdown Pool Adder Lambda for instance {}: {}", instanceId, response.toString());
                        } catch (Exception e) {
                            log.error("Error in asynchronous Lambda call for instance {}. \n{}", instanceId, e.getMessage());
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
