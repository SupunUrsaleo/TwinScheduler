package com.ursaleo.twin.scheduler.Service;


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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Service
@Slf4j
public class SchedulerService {

    @Autowired
    private TwinAvailabilityRepository twinAvailabilityRepository;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Autowired
    TwinHandlerService twinHandlerService;

    @Value("${endpoints.healthcheck}")
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
    private long autoScaleImageName;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    public void scheduleTasks() {
        log.info("Starting schedulers..");
        scheduledExecutorService.scheduleAtFixedRate(this::performAutoScaling, 0, autoScaleRate, TimeUnit.MILLISECONDS);
        scheduledExecutorService.scheduleAtFixedRate(this::performHealthCheck, 0, healthCheckRate, TimeUnit.MILLISECONDS);
    }
    public void performHealthCheck() {
        log.info("Starting health check.");
        try {
        String json = restTemplate.getForObject(healthCheckEndpoint, String.class);
        log.info("Received health check response : "+json);

        } catch (RestClientException e) {
            log.error("Failed to perform health check", e);
        }
        log.info("End of health check.");

    }

    public void performAutoScaling() {
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
                System.out.println("Response from AutoScaler: " + body);

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
