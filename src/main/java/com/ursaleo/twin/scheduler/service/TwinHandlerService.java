package com.ursaleo.twin.scheduler.service;

import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.exception.TwinSchedulerException;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.model.TwinAvailability;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

    @Value("${scheduler.autoscale.imageName}")
    private String autoScaleImageName;

    @Autowired
    private AppSessionRepository appSessionRepository;

    @Autowired
    private TwinAvailabilityRepository twinAvailabilityRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    void invokeTwinHealthCheck(JSONArray instanceIds, String twinVersionId) {

        for (int attempt = 1; attempt <= retries; attempt++) {
            try {
                JSONObject requestObj = new JSONObject();
                requestObj.put("instance_ids", instanceIds);

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<String> entity = new HttpEntity<>(requestObj.toString(), headers);
                ResponseEntity<String> response = restTemplate.postForEntity(new URI(healthCheckEndpoint), entity, String.class);

                if (response.getStatusCode().is2xxSuccessful()) {
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


    private void updateAllAppSessions(String twinVersionId, JSONArray responseArr) throws JSONException {
        JSONArray failedInstances = new JSONArray();
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
            } else if (port.equals(Status.STARTING)) {
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
                appSessionRepository.save(appSession);
            }//TODO: what if the lambda fails? how should we handle it.
        }
    }

    public String createTwinStream(JSONObject requestObject) throws JSONException {

        String twinVersionId = requestObject.getString("twinVersionId");
        String partnerSecureData = requestObject.getString("data");
        TwinAvailability byTwinVersionId = twinAvailabilityRepository.findByTwinVersionId(twinVersionId);
        if(Objects.isNull(byTwinVersionId)){
            throw new TwinSchedulerException("No defined Twin version id found.");
        }

        List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
        String url = "Could not Create URL, Please try again later.";
        if(byTwinVersionIdAndStatus.isEmpty()){

            List<AppSession> busyTwinSessions = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId,Status.BUSY);
            if(byTwinVersionId.getMaxBusy()==busyTwinSessions.size()){
                throw new TwinSchedulerException("Max Limit of instances for this Twin version is reached. No new instances will be spawned.");
            }
            //Spawn new instance.
            JSONArray newInstanceArr = startInstances(1);
            invokeTwinHealthCheck(newInstanceArr, twinVersionId);
            byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
            if(!byTwinVersionIdAndStatus.isEmpty()){
                url = getStreamUrl(byTwinVersionIdAndStatus.get(0), partnerSecureData);
            }
        }else{
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
}
