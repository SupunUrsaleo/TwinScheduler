package com.ursaleo.twin.scheduler.service;

import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.exception.TwinSchedulerException;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.model.ShutdownPool;
import com.ursaleo.twin.scheduler.model.TwinAvailability;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.ShutdownPoolRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
// import org.apache.logging.log4j.CloseableThreadContext.Instance;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;

import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;

@Service
@Slf4j
public class TwinHandlerService {

    @Value("${endpoints.lambda.healthcheck}")
    public String healthCheckEndpoint;

    @Value("${endpoints.lambda.healthcheck.retries}")
    public int retries;

    @Value("${endpoints.lambda.healthcheck.delay}")
    public int delay;

    @Value("${extractor.application.port}")
    int extractorPort;

    @Value("${endpoints.autoscale}")
    private String autoScaleEndpoint;

    @Value("${endpoints.autostart}")
    private String autoStartEndpoint;

    @Value("${endpoints.autostop}")
    private String autoStopEndpoint;

    @Value("${endpoints.statecheck}")
    private String stateCheckerEndpoint;

    @Value("${endpoints.autoterminate}")
    private String autoTerminateEndpoint;

    @Value("${endpoints.shutdownpooladder}")
    private String shutdownPoolEndpoint;

    @Value("${scheduler.autoscale.imageName}")
    private String autoScaleImageName;

    @Value("${scheduler.autostart.minShutdownInstances}")
    private int autoStartMinShutdownInstances;

    @Autowired
    private AppSessionRepository appSessionRepository;

    @Autowired
    private TwinAvailabilityRepository twinAvailabilityRepository;

    @Autowired
    private ShutdownPoolRepository shutdownPoolRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    void invokeTwinHealthCheck(JSONArray instanceIds, String twinVersionId) {

        for (int attempt = 1; attempt <= retries; attempt++) {
            try {
                JSONObject requestObj = new JSONObject();
                requestObj.put("instance_ids", instanceIds);

                // Create the data object
                JSONObject dataObj = new JSONObject();
                JSONObject partnerSecureDataObj = new JSONObject();
                JSONObject userDataObj = new JSONObject();

                userDataObj.put("clientId", "0ee42e0b-9aca-4b10-865b-83ec932ae7c2");
                userDataObj.put("twinId", "9e3cd296-7d38-4a63-9c69-fa2d6f570887");
                userDataObj.put("twinVersionId",  twinVersionId);
                userDataObj.put("baseUrl", "https://app.ursaleo.com");
                partnerSecureDataObj.put("app_data", userDataObj);
                dataObj.put("partnerSecureData", partnerSecureDataObj);
                requestObj.put("data", dataObj);


                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<String> entity = new HttpEntity<>(requestObj.toString(), headers);
                ResponseEntity<String> response = restTemplate.postForEntity(new URI(healthCheckEndpoint), entity, String.class);

                if (response.getStatusCode().is2xxSuccessful()) {
                    log.error("The healthcheck passed for specified Instances {} statuscode: {}", instanceIds.toString(),response.getStatusCode());
                    updateAllAppSessions(twinVersionId, new JSONArray(response.getBody()));
                    return;
                } else {
                    log.error("The healthcheck failed for specified Instances {}", instanceIds.toString());
                    updateExistingAppSessions(instanceIds);
                    log.error("Updated the Instances {} as dead.", instanceIds.toString());
                    return;
                }
            } catch (Exception e) {
                log.error("Attempt {} failed: {}", attempt, e.getMessage());

                if (attempt < retries) {
                    try {
                        Thread.sleep(delay); // Wait for 5 seconds before retrying
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Thread was interrupted during sleep", ie);
                        return;
                    }
                } else {
                    log.error("All attempts to invoke health check failed.");
                }
            }
        }
    }

    public void invokeTwinStoppedCheck(JSONArray instancesForCheckStopped) throws JSONException {
        JSONArray failedInstances = new JSONArray();  // For instances that fail the process
        
        for (int i = 0; i < instancesForCheckStopped.length(); i++) {
            String instanceId = instancesForCheckStopped.getString(i);
            
            // Check if the instance already exists in the ShutdownPool table
            ShutdownPool shutdownPool = shutdownPoolRepository.findByInstanceId(instanceId);
    
            if (shutdownPool == null) {
                // If the entry doesn't exist, create a new one
                shutdownPool = new ShutdownPool();
                shutdownPool.setInstanceId(instanceId);
            }
    
            // Check the current status of the instance
            String currentStatus = checkInstanceState(instanceId);  // This method will check the current state via Lambda or local method
    
            // Handle different statuses
            if ("stopping".equalsIgnoreCase(currentStatus)) {
                // log.info("Instance {} is stopping, adding it to ShutdownPool", instanceId);
                shutdownPool.setStatus(Status.STOPPING);  // Set the status to "stopping" or "stopped"
                shutdownPoolRepository.save(shutdownPool);
            } else if ("stopped".equalsIgnoreCase(currentStatus)) {
                // log.info("Instance {} is stopping, adding it to ShutdownPool", instanceId);
                shutdownPool.setStatus(Status.STOPPED);  // Set the status to "stopping" or "stopped"
                shutdownPoolRepository.save(shutdownPool);
            } else if ("pending".equalsIgnoreCase(currentStatus)  || "running".equalsIgnoreCase(currentStatus)) {
                // log.info("Instance {} has started from ShutdownPool", instanceId);
                shutdownPool.setStatus(Status.STARTED); 
                shutdownPoolRepository.save(shutdownPool);
            } else if ("shutting-down".equalsIgnoreCase(currentStatus) || "terminated".equalsIgnoreCase(currentStatus)) {
                // log.info("Instance {} is shutting down or terminated, dead it from ShutdownPool", instanceId);
                shutdownPool.setStatus(Status.DEAD);
                shutdownPoolRepository.save(shutdownPool); 
            } else {
                log.error("Instance {} has an invalid status: {}", instanceId, currentStatus);
                failedInstances.put(instanceId);  // Log instances with invalid status
                continue;
            }
        }
                // //If we want to Remove the instance from the ShutdownPool table
                // shutdownPoolRepository.delete(shutdownPool);
    
        if (failedInstances.length() > 0) {
            log.error("Failed to update instances: {}", failedInstances.toString());
        }
    }
    

    private void updateAllAppSessions(String twinVersionId, JSONArray responseArr) throws JSONException {
        JSONArray failedInstances = new JSONArray(); //TODO: if the instance is dead no use keeping it. shut them down
        for (int i = 0; i < responseArr.length(); i++) {
            JSONObject responseObj = responseArr.getJSONObject(i);
            String instanceID = responseObj.getString("InstanceId");
            if(!Objects.equals(responseObj.getString("Status"), "running")){
               failedInstances.put(instanceID);
               continue;
            }
            AppSession appSession = appSessionRepository.findByInstanceID(instanceID);

            if(Objects.isNull(appSession)){
                appSession = new AppSession();
                appSession.setSessionId(UUID.randomUUID());
                appSession.setInstanceID(instanceID);
            }
            appSession.setServerPublicIP(responseObj.getString("PublicIP"));
            appSession.setServerPrivateIP(responseObj.getString("PrivateIP"));
            appSession.setTwinVersionId(twinVersionId);

            String port = responseObj.getString("Port");

            //appSession.setPartnerSecureData("secureData");
           // appSession.setStartTime(LocalDateTime.parse(responseObj.getString("LaunchTime")));
            //appSession.setEndTime(LocalDateTime.now().plusMinutes(1));
            
            if(Objects.isNull(appSession.getStatus()) || !appSession.getStatus().equals(Status.BUSY)){
                appSession.setStatus(Status.AVAILABLE);
            }

            if(StringUtils.isNumeric(port)){
                appSession.setMappedPort(Integer.parseInt(port));
            } else {
                appSession.setStatus(Status.STARTING);
            }            

            appSessionRepository.save(appSession);
        }

        if(failedInstances.length() > 0){
            updateExistingAppSessions(failedInstances);
        }

    }

    private void updateExistingAppSessions(JSONArray instanceIDs) throws JSONException {
        for (int i = 0; i < instanceIDs.length(); i++) {
            String instanceID = instanceIDs.getString(i);
            AppSession appSession = appSessionRepository.findByInstanceID(instanceID);
            if(Objects.nonNull(appSession)){
                appSession.setStatus(Status.DEAD);
                log.info("Updated as dead {}",instanceID);
                appSessionRepository.save(appSession);
            }//TODO: what if the lambda fails? how should we handle it.
        }
    }

    // public String createTwinStream(JSONObject requestObject) throws JSONException {

    //     String twinVersionId = requestObject.getString("twinVersionId");
    //     String partnerSecureData = requestObject.getString("data");
    //     TwinAvailability byTwinVersionId = twinAvailabilityRepository.findByTwinVersionId(twinVersionId);
    //     if(Objects.isNull(byTwinVersionId)){
    //         throw new TwinSchedulerException("No defined Twin version id found.");
    //     }

    //     List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
    //     String url = "Could not Create URL, Please try again later.";
    //     if(byTwinVersionIdAndStatus.isEmpty()){

    //         List<AppSession> busyTwinSessions = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId,Status.BUSY);
    //         if(byTwinVersionId.getMaxBusy()<=busyTwinSessions.size()){
    //             throw new TwinSchedulerException("Max Limit of instances for this Twin version is reached. No new instances will be spawned.");
    //         }
    //         //Spawn new instance.
    //         JSONArray newInstanceArr = startInstances(1);
    //         invokeTwinHealthCheck(newInstanceArr, twinVersionId);
    //         byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
    //         if(!byTwinVersionIdAndStatus.isEmpty()){
    //             url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
    //         }
    //     }else{
    //         url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
    //     }

    //     return url;
    // }

    public String createTwinStream(JSONObject requestObject) throws JSONException {

        String twinVersionId = requestObject.getString("twinVersionId");
        String partnerSecureData = requestObject.getString("data");
        TwinAvailability byTwinVersionId = twinAvailabilityRepository.findByTwinVersionId(twinVersionId);
        if (Objects.isNull(byTwinVersionId)) {
            throw new TwinSchedulerException("No defined Twin version id found.");
        }

        // Retrieve available instances by twinVersionId
        List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
        String url = "Could not Create URL, Please try again later.";

        // If no available instances, try to start one from the shutdown pool
        if (byTwinVersionIdAndStatus.isEmpty()) {

            // Retrieve busy twin sessions
            List<AppSession> busyTwinSessions = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.BUSY);

            // Check if max busy instances limit has been reached
            if (byTwinVersionId.getMaxBusy() <= busyTwinSessions.size()) {
                throw new TwinSchedulerException("Max Limit of instances for this Twin version is reached. No new instances will be spawned.");
            }

            // Get one instance from the ShutdownPool
            List<ShutdownPool> stoppedInstances = shutdownPoolRepository.findByStatus(Status.STOPPED);

            if (!stoppedInstances.isEmpty()) {
                // Get the first available instance from the stopped pool
                ShutdownPool selectedInstance = stoppedInstances.get(0);
                String instanceId = selectedInstance.getInstanceId();

                // Immediately update the status to 'STARTING' to lock the instance
                selectedInstance.setStatus(Status.STARTED);
                shutdownPoolRepository.save(selectedInstance);  // Save the status update
                // shutdownPoolRepository.delete(selectedInstance);

                JSONArray instancesToStartArray = new JSONArray();
                instancesToStartArray.put(instanceId);

                log.info("Starting instance {} from StoppedInstanceIds", instanceId);

                // Start the selected instance
                JSONArray startedInstances = startEC2Instances(instancesToStartArray);

                // Add a delay before calling the health check
                try {
                    Thread.sleep(1000); // Wait for 1 seconds (adjust as needed)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TwinSchedulerException("Interrupted while waiting for instance to transition states.");
                }                

                // Call the health check after starting the instance
                log.info("Starting invokeTwinHealthCheck for instance {} from StoppedInstanceIds", instanceId);
                invokeTwinHealthCheck(instancesToStartArray, twinVersionId);  // Call health check
                // // Log the status of the started instance
                // if (startedInstances.length() > 0) {
                //     JSONObject instance = startedInstances.getJSONObject(0);
                //     log.info("Started EC2 instance: instanceId={}, status={}", instance.getString("instanceId"), instance.getString("status"));
                //     byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
                // }

                            // Retry mechanism: wait and check if the instance is available
                int maxRetries = 10;  // Maximum retries (e.g., wait for 5 minutes total with 30-second intervals)
                int retryCount = 0;
                while (retryCount < maxRetries) {
                    log.info("Checking for available instance (attempt {})...", retryCount + 1);
                    byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
                    if (!byTwinVersionIdAndStatus.isEmpty()) {
                        log.info("Instance is now available.");
                        break;
                    }
                    retryCount++;
                    try {
                        Thread.sleep(30000);  // Wait for 5 seconds before retrying
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TwinSchedulerException("Interrupted while waiting for instance to become available.");
                    }
                }

                // Check if an available instance is present after starting
                if (!byTwinVersionIdAndStatus.isEmpty()) {
                    url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
                } else {
                    throw new TwinSchedulerException("No available instances found after starting stopped instance.");
                }
            } else {
                // If no stopped instances are available, fallback to spawning a new instance
                log.info("No stopped instances available, spawning a new one.");
                JSONArray newInstanceArr = startInstances(1);  // Start one new instance
                invokeTwinHealthCheck(newInstanceArr, twinVersionId);
                byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
                if (!byTwinVersionIdAndStatus.isEmpty()) {
                    url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
                }
            }
        } else {
            // If available instances are found, use the first one
            url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
        }

        return url;
    }


    private String getStreamUrl(AppSession appSession, String partnerSecureData) {
        String serverPublicIP = appSession.getServerPublicIP();
        int mappedPort = appSession.getMappedPort();
        HttpStatusCode status = updatePartnerSecureData(partnerSecureData, serverPublicIP, mappedPort);
        if(status.is2xxSuccessful()){
            String url = String.format("http://%s:%s/streaming/webrtc-demo/?server=%s", serverPublicIP,8011,serverPublicIP);
            appSession.setStatus(Status.BUSY);
            log.info("Sending Partner Secure Data to : {}",url);
            appSessionRepository.save(appSession);
            return url;
        }
        throw new TwinSchedulerException("Could not Save provided Partner Data. Please try again.");
    }

    private HttpStatusCode updatePartnerSecureData(String partnerSecureData, String serverPublicIP, int mappedPort){
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<>(partnerSecureData, headers);
        URI uri = null;
        try {
            uri = new URI(String.format("http://%s:%d/api/savePartnerData", serverPublicIP, 8081));
            log.info("Sending Partner Secure Data to : {}",uri.toString());
            ResponseEntity<String> response = restTemplate.postForEntity(uri, entity, String.class);
            return response.getStatusCode();
        } catch (Exception e) {
            log.error("Could not save Partner Secure Data, Error: {}",
                    e.getMessage(),e);
            throw new TwinSchedulerException("Could not save Partner Secure Data.");
        }

    }

    public JSONArray startInstances(int numberOfInstances) throws JSONException {
        ResponseEntity<String> response = restTemplate.postForEntity(getAutoScalerURI(numberOfInstances), null, String.class);
        JSONArray newInstances = new JSONArray();
        if (response.getStatusCode().is2xxSuccessful()) {
            String body = response.getBody();
            log.info("Response from AutoScaler: {}", body);
            JSONObject jsonObject = new JSONObject(body);
            newInstances = jsonObject.getJSONArray("instanceIds");
        } else {
            log.error("Autoscaling Request failed with status code: " + response.getStatusCode());
        }
        return newInstances;
    }


    public URI getAutoScalerURI(int numberOfInstances){

        return UriComponentsBuilder
                .fromHttpUrl(autoScaleEndpoint)
                .queryParam("imageName", autoScaleImageName)
                .queryParam("instances", numberOfInstances)
                .build().toUri();
    }

    public String releaseTwin(String publicIp) {
        AppSession byServerPublicIP = appSessionRepository.findByServerPublicIP(publicIp);
        if(Objects.nonNull(byServerPublicIP)){
            byServerPublicIP.setStatus(Status.AVAILABLE);
            appSessionRepository.save(byServerPublicIP);
            return HttpStatus.OK.getReasonPhrase();
        }else{
            return "IP not found.";
        }
    }

    public JSONArray startEC2Instances(JSONArray instanceIds) throws JSONException {
        // Convert JSONArray of instanceIds into a query parameter or pass as body (depending on your API design)
        ResponseEntity<String> response = restTemplate.postForEntity(getAutoStartURI(instanceIds), null, String.class);
        JSONArray newInstances = new JSONArray();
    
        if (response.getStatusCode().is2xxSuccessful()) {
            String body = response.getBody();
            log.info("Response from AutoStart Lambda: {}", body);
            
            // Directly convert the body to JSONArray (since that's what your Lambda is returning)
            newInstances = new JSONArray(body);
        } else {
            log.error("AutoStart Request failed with status code: " + response.getStatusCode());
        }
        return newInstances;
    }
    

    public URI getAutoStartURI(JSONArray instanceIds) throws JSONException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(autoStartEndpoint);
    
        // Join the instanceIds into a comma-separated string
        StringBuilder instanceIdParam = new StringBuilder();
        for (int i = 0; i < instanceIds.length(); i++) {
            if (i > 0) {
                instanceIdParam.append(",");
            }
            instanceIdParam.append(instanceIds.getString(i));
        }
    
        // Add the comma-separated instanceId string as a single query parameter
        builder.queryParam("instanceId", instanceIdParam.toString());
    
        return builder.build().toUri();
    }
    
    public JSONArray stopEC2Instances(JSONArray instanceIds) throws JSONException {
        // Convert JSONArray of instanceIds into a query parameter or pass as body (depending on your API design)
        ResponseEntity<String> response = restTemplate.postForEntity(getAutoStopURI(instanceIds), null, String.class);
        JSONArray stoppedInstances = new JSONArray();
    
        if (response.getStatusCode().is2xxSuccessful()) {
            String body = response.getBody();
            log.info("Response from AutoStop Lambda: {}", body);
            
            // Directly convert the body to JSONArray (since that's what your Lambda is returning)
            stoppedInstances = new JSONArray(body);
        } else {
            log.error("AutoStop Request failed with status code: " + response.getStatusCode());
        }
        return stoppedInstances;
    }
    
    public URI getAutoStopURI(JSONArray instanceIds) throws JSONException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(autoStopEndpoint);
    
        // Join the instanceIds into a comma-separated string
        StringBuilder instanceIdParam = new StringBuilder();
        for (int i = 0; i < instanceIds.length(); i++) {
            if (i > 0) {
                instanceIdParam.append(",");
            }
            instanceIdParam.append(instanceIds.getString(i));
        }
    
        // Add the comma-separated instanceId string as a single query parameter
        builder.queryParam("instanceId", instanceIdParam.toString());
    
        return builder.build().toUri();
    }

    public String checkInstanceState(String instanceId) throws JSONException {
        // Build the URI for the state-checking Lambda
        URI uri = getInstanceStateURI(instanceId);
        
        // Send the request to the Lambda and get the response
        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
        
        if (response.getStatusCode().is2xxSuccessful()) {
            String body = response.getBody();
            // log.info("Response from EC2StateChecker Lambda: {}", body);
            
            // Convert the response body to a JSON object to extract the instance state
            JSONArray responseArray = new JSONArray(body);
            if (responseArray.length() > 0) {
                JSONObject instanceData = responseArray.getJSONObject(0);
                String state = instanceData.getString("state");  // Extract the 'state' field from JSON
                
                return state;
            } else {
                log.error("No state information found for instance {}", instanceId);
                return "unknown";
            }
        } else {
            log.error("EC2StateChecker Lambda Request failed with status code: {}", response.getStatusCode());
            return "unknown";
        }
    }
    
    public URI getInstanceStateURI(String instanceId) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(stateCheckerEndpoint);
        
        // Add the instanceId as a query parameter
        builder.queryParam("instanceId", instanceId);
        
        return builder.build().toUri();
    }

    // public void shutdownInstanceByPublicIp(String publicIp) {
    //     try {
    //         // Step 1: Retrieve the instanceId from the appSession table using the publicIp
    //         AppSession appSession = appSessionRepository.findByServerPublicIP(publicIp);
    
    //         if (appSession == null) {
    //             log.error("No appSession found for public IP: {}", publicIp);
    //             return;
    //         }

    //         // Ensure shutdown only happens if appSession status is BUSY
    //         if (!appSession.getStatus().equals(Status.BUSY)) {
    //             log.info("AppSession for public IP {} is not in BUSY status. Shutdown skipped.", publicIp);
    //             return;
    //         }

    //         String instanceId = appSession.getInstanceID();
    //         log.info("Found instance ID {} for public IP {}", instanceId, publicIp);
    
    //         // Step 2: Check the number of shutdown instances in the ShutdownPool
    //         List<ShutdownPool> shutdownInstances = shutdownPoolRepository.findByStatus(Status.STOPPED); // Also add stopping state if necessary
    
    //         JSONArray instanceIds = new JSONArray();
    //         instanceIds.put(instanceId);
    
    //         if (shutdownInstances.size() >= autoStartMinShutdownInstances) {
    //             // If min_reserved shutdown instances are already present, terminate the instance
    //             log.info("Min reserved shutdown instances reached. Terminating instance ID {}", instanceId);
    
    //             // Call the Lambda to terminate the instance
    //             // terminateEC2Instances(instanceIds);

    //             // Stop the instance but this should be replaced by terminateEC2Instances(instanceIds);
    //             stopEC2Instances(instanceIds);  // Call the method to stop the instance
    
    //             // Mark the app session as dead after termination
    //             appSession.setStatus(Status.DEAD);
    //             appSessionRepository.save(appSession);
    //             log.info("AppSession for instance ID {} has been updated to DEAD after termination.", instanceId);
    
    //         } else {
    //             // Otherwise, stop the instance and add it to the ShutdownPool
    //             log.info("Stopping instance ID {} and adding it to ShutdownPool", instanceId);
    
    //             // Stop the instance
    //             stopEC2Instances(instanceIds);  // Call the method to stop the instance
    
    //             // Add to ShutdownPool
    //             JSONArray instancesForCheckStopped = new JSONArray();
    //             instancesForCheckStopped.put(instanceId);
    
    //             // Invoke the twin stop check to update the ShutdownPool
    //             invokeTwinStoppedCheck(instancesForCheckStopped);
    
    //             // Mark the app session as dead after stopping
    //             appSession.setStatus(Status.DEAD);
    //             appSessionRepository.save(appSession);
    //             log.info("AppSession for instance ID {} has been updated to DEAD after stopping.", instanceId);
    //         }
    
    //     } catch (Exception e) {
    //         log.error("Error shutting down the instance for public IP {}: {}", publicIp, e.getMessage());
    //     }
    // }

    //shutdownInstanceByPublicIp should be replace by this
    public void shutdownInstanceByPublicIp(String publicIp) {
        try {
            // Step 1: Retrieve the instanceId from the appSession table using the publicIp
            AppSession appSession = appSessionRepository.findByServerPublicIP(publicIp);

            if (appSession == null) {
                log.error("No appSession found for public IP: {}", publicIp);
                return;
            }

            // Ensure shutdown only happens if appSession status is BUSY or DEAD
            if (!appSession.getStatus().equals(Status.BUSY) && !appSession.getStatus().equals(Status.DEAD)) {
                log.info("AppSession for public IP {} is not in BUSY or DEAD status. Shutdown skipped.", publicIp);
                return;
            }

            String instanceId = appSession.getInstanceID();
            log.info("Found instance ID {} for public IP {}", instanceId, publicIp);

            // Step 2: Check if the instance is in the ShutdownPool
            ShutdownPool shutdownPoolEntry = shutdownPoolRepository.findByInstanceId(instanceId);

            if (shutdownPoolEntry != null) {
                // Instance is in the ShutdownPool; stop it
                log.info("Instance ID {} is in the ShutdownPool. Stopping the instance.", instanceId);

                JSONArray instanceIds = new JSONArray();
                instanceIds.put(instanceId);

                // Stop the instance
                stopEC2Instances(instanceIds);

                // Update ShutdownPool status to reflect stopped state
                shutdownPoolEntry.setStatus(Status.STOPPING);
                shutdownPoolRepository.save(shutdownPoolEntry);

                // Mark the app session as DEAD
                appSession.setStatus(Status.DEAD);
                appSessionRepository.save(appSession);
                log.info("AppSession for instance ID {} has been updated to DEAD due to User Inactivity.", instanceId);
            } else {
                // Instance is not in the ShutdownPool; terminate it
                log.info("Instance ID {} is not in the ShutdownPool. Terminating the instance.", instanceId);

                JSONArray instanceIds = new JSONArray();
                instanceIds.put(instanceId);

                // Terminate the instance
                terminateEC2Instances(instanceIds);

                // Mark the app session as DEAD
                appSession.setStatus(Status.DEAD);
                appSessionRepository.save(appSession);
                log.info("AppSession for instance ID {} has been updated to DEAD after due to User Inactivity.", instanceId);
            }
        } catch (Exception e) {
            log.error("Error shutting down or terminating the instance for public IP {}: {}", publicIp, e.getMessage());
        }
        return;
    }

    public JSONArray terminateEC2Instances(JSONArray instanceIds) throws JSONException {
        // Convert JSONArray of instanceIds into a query parameter or pass as body (depending on your API design)
        ResponseEntity<String> response = restTemplate.postForEntity(getAutoTerminateURI(instanceIds), null, String.class);
        JSONArray terminatedInstances = new JSONArray();
    
        if (response.getStatusCode().is2xxSuccessful()) {
            String body = response.getBody();
            log.info("Response from AutoTerminate Lambda: {}", body);
            
            // Directly convert the body to JSONArray (since that's what your Lambda is returning)
            terminatedInstances = new JSONArray(body);
        } else {
            log.error("AutoTerminate Request failed with status code: " + response.getStatusCode());
        }
        return terminatedInstances;
    }
    
    public URI getAutoTerminateURI(JSONArray instanceIds) throws JSONException {
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(autoTerminateEndpoint);
    
        // Join the instanceIds into a comma-separated string
        StringBuilder instanceIdParam = new StringBuilder();
        for (int i = 0; i < instanceIds.length(); i++) {
            if (i > 0) {
                instanceIdParam.append(",");
            }
            instanceIdParam.append(instanceIds.getString(i));
        }
    
        // Add the comma-separated instanceId string as a single query parameter
        builder.queryParam("instanceId", instanceIdParam.toString());
    
        return builder.build().toUri();
    }

    public JSONArray ShutdownPoolAdder(JSONArray instanceIds) throws JSONException {
    JSONArray shutdownResults = new JSONArray();

    for (int i = 0; i < instanceIds.length(); i++) {
        String instanceId = instanceIds.getString(i);

        try {
            // Construct the Lambda API URI with the instanceId as a query parameter
            String uri = String.format("%s?instanceId=%s", shutdownPoolEndpoint, instanceId);

            // Send the GET request to the Lambda API
            ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                // Parse the response body (assuming it's JSON)
                String body = response.getBody();
                log.info("Response from Shutdown Pool Adder Lambda for instance {}: {}", instanceId, body);

                // Add the response JSON to the results array
                shutdownResults.put(new JSONObject(body));
            } else {
                log.error("Shutdown Pool Adder requested for instance {}: {}", instanceId, response.getStatusCode());
                shutdownResults.put(new JSONObject()
                        .put("instanceId", instanceId)
                        .put("error", "Request failed with status code: " + response.getStatusCode()));
            }
        } catch (Exception e) {
            log.error("Error while calling Shutdown Pool Adder Lambda for instance {}: {}", instanceId, e.getMessage());
            shutdownResults.put(new JSONObject()
                    .put("instanceId", instanceId)
                    .put("error", e.getMessage()));
        }
    }

    return shutdownResults;
}

}
