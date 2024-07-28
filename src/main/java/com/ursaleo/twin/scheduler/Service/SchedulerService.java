package com.ursaleo.twin.scheduler.Service;


import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import com.ursaleo.twin.scheduler.repository.TwinAvailabilityRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;

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

    @Value("${endpoints.autoscale}")
    private String autoScaleEndpoint;

    @Value("${scheduler.healthcheck.fixedRate}")
    private long healthCheckRate;

    @Value("${scheduler.autoscale.fixedRate}")
    private long autoScaleRate;

    @Value("${scheduler.autoscale.minInstances}")
    private long autoScaleMinInstances;


    @Value("${scheduler.autoscale.imageName}")
    private String autoScaleImageName;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    public void scheduleTasks() {
        log.info("Starting schedulers..");
        scheduledExecutorService.scheduleAtFixedRate(this::performAutoScaling, 0, autoScaleRate, TimeUnit.MILLISECONDS);
        //scheduledExecutorService.scheduleAtFixedRate(this::performHealthCheck, 0, healthCheckRate, TimeUnit.MILLISECONDS); //TODO: do this from the autoscaler.
    }
    public void performHealthCheck() {
        //TODO: should also check if the application inside the instance is up and restart the instance if the server is down.
        log.info("Starting health check.");

        List<AppSession> aliveInstances = appSessionRepository.findByStatus("Alive");
        List<String> instanceIds = aliveInstances.stream()
                .map(AppSession::getInstanceID)
                .toList();

        if(instanceIds.isEmpty()){
            return;
        }
        JSONObject requestObj = new JSONObject();
        try {
            requestObj.put("instance_ids",new JSONArray(instanceIds));
        ResponseEntity<String> response = restTemplate.postForEntity(new URI(healthCheckEndpoint),requestObj.toString() , String.class); //TODO: modify Healthcheck endpoint to check the application inside the instance and start if its not started. should also return the twinID.

        if(response.getStatusCode().is2xxSuccessful()){
            JSONArray responseArr = new JSONArray(response.getBody());
            for (int i = 0; i < responseArr.length(); i++) {
                JSONObject responseObj = responseArr.getJSONObject(i);
                AppSession appSession = appSessionRepository.findByInstanceID(responseObj.getString("InstanceId"));
                appSession.setStatus(responseObj.getString("Status"));
                appSessionRepository.save(appSession);
            }

        }else{
            log.error("Health check failed for instances {}",instanceIds);
        }

        } catch (JSONException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        aliveInstances.forEach(instance ->{
            try {
                String json = restTemplate.getForObject(healthCheckEndpoint, String.class);
                log.info("Received health check response : "+json);

            } catch (RestClientException e) {
                log.error("Failed to perform health check", e);
            }

        });


        log.info("End of health check.");

    }

    public void performAutoScaling() {


        performHealthCheck();

        log.info("Starting TwinAutoscaler..");
        try{

            URI uri = UriComponentsBuilder.fromHttpUrl(autoScaleEndpoint)
                    .queryParam("imageName", autoScaleImageName)
                    .queryParam("minInstances", autoScaleMinInstances)
                    .build()
                    .toUri();

            ResponseEntity<String> response = restTemplate.postForEntity(uri, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                String body = response.getBody();
                log.info("Response from AutoScaler: " + body);

                JSONObject jsonObject = new JSONObject(body);
                JSONArray instanceIds = jsonObject.getJSONArray("instanceIds");
                if(instanceIds.length() > 0){
                    twinHandlerService.invokeTwinHealthCheck(instanceIds);
                }else{
                    log.info("Twin Autoscaling completed, No new instances were spawned.");
                }

            } else {
                log.error("Autoscaling Request failed with status code: " + response.getStatusCode());
            }

        } catch (RestClientException e) {
            log.error( "Failed to perform health check", e);
        } catch (JSONException e) {
            log.error("Failed to perform Parse result", e);
        }
        log.info("Twin Autoscaling complete.");
    }


}
