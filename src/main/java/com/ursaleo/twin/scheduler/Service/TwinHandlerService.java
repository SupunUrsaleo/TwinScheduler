package com.ursaleo.twin.scheduler.Service;

import com.ursaleo.twin.scheduler.config.Status;
import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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

    @Value("${twin.application.name}")
    String twinAppName;

    @Value("${extractor.application.port}")
    int extractorPort;

    @Value("${endpoints.autoscale}")
    private String autoScaleEndpoint;

    @Value("${scheduler.autoscale.imageName}")
    private String autoScaleImageName;

    @Autowired
    private AppSessionRepository appSessionRepository;

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
            appSession.setServerIP(responseObj.getString("PublicIP"));
            appSession.setTwinVersionId(twinVersionId);
            //appSession.setPartnerSecureData("secureData");
           // appSession.setStartTime(LocalDateTime.parse(responseObj.getString("LaunchTime")));
            //appSession.setEndTime(LocalDateTime.now().plusMinutes(1));
            appSession.setMappedPort(responseObj.getInt("Port"));
            appSession.setStatus(Status.AVAILABLE);
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
        List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
        String url = "Could not Create URL, Please try again later.";
        if(byTwinVersionIdAndStatus.isEmpty()){
            //Spawn new instance.
            JSONArray newInstanceArr = startInstances(1);
            invokeTwinHealthCheck(newInstanceArr, twinVersionId);
            byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(twinVersionId, Status.AVAILABLE);
            if(!byTwinVersionIdAndStatus.isEmpty()){
                url = getStreamUrl(byTwinVersionIdAndStatus.get(0));
            }
        }else{
            //TODO add partnersecure data and create the URL ex http://13.202.129.194:8011/streaming/webrtc-demo/?server=13.202.129.194
            url = getStreamUrl(byTwinVersionIdAndStatus.get(0));
        }

        return url;
    }

    private String getStreamUrl(AppSession appSession) {
        String url = String.format("http://%s:8011/streaming/webrtc-demo/?server=%s", appSession.getServerIP(), appSession.getServerIP());
        appSession.setStatus("BUSY");
        appSessionRepository.save(appSession);
        return url;
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
                .queryParam("minInstances", numberOfInstances)
                .build().toUri();
    }

}
