package com.ursaleo.twin.scheduler.Service;

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
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.isNumeric;

@Service
@Slf4j
public class TwinHandlerService {

    @Value("${endpoints.lambda.healthcheck}")
    public String healthCheckEndpoint;

    @Value("${twin.application.name}")
    String twinAppName;

    @Value("${extractor.application.port}")
    int extractorPort;

    @Autowired
    private AppSessionRepository appSessionRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    void invokeTwinHealthCheck(JSONArray instanceIds) {

        try {
            JSONObject requestObj = new JSONObject();
            requestObj.put("instance_ids",instanceIds);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = new HttpEntity<>(requestObj.toString(), headers);
            Thread.sleep(60000);
            ResponseEntity<String> response = restTemplate.postForEntity(new URI(healthCheckEndpoint),entity , String.class);
            if(response.getStatusCode().is2xxSuccessful()){
                JSONArray responseArr = new JSONArray(response.getBody());

                for (int i = 0; i < responseArr.length(); i++) {
                    JSONObject responseObj = responseArr.getJSONObject(i);
                    AppSession appSession = new AppSession();
                    appSession.setSessionId(UUID.randomUUID()); //TODO: should this be the instanceID?
                    appSession.setServerIP(responseObj.getString("PublicIP"));
                    appSession.setInstanceID(responseObj.getString("InstanceId"));
                    //appSession.setPartnerSecureData("secureData");
                   // appSession.setStartTime(LocalDateTime.parse(responseObj.getString("LaunchTime")));
                    //appSession.setEndTime(LocalDateTime.now().plusMinutes(1));
                    invokeTwinExtractor(appSession);
                }

            }else {
                log.error("The healthcheck failed for specified Instances {}",instanceIds.toString() );
            }

        } catch (URISyntaxException | JSONException | InterruptedException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }


    }

    private void invokeTwinExtractor(AppSession appSession) {

        /*URI uri = UriComponentsBuilder.fromHttpUrl("http://"+appSession.getServerIP()+":"+extractorPort+"/getPort")
                .queryParam("appName", twinAppName)
                .build()
                .toUri();*/

        URI uri = UriComponentsBuilder.fromHttpUrl("http://54.243.17.13:8080/getPort")//TODO: hardcoded for testing.
                .queryParam("appName", twinAppName)
                .build()
                .toUri();

        ResponseEntity<String> responseEntity = restTemplate.getForEntity(uri, String.class);

        if(responseEntity.getStatusCode().is2xxSuccessful()){

            String responseString = responseEntity.getBody();
            int port = isNumeric(responseString) ? Integer.parseInt(responseString) : -1;
            log.info("Received twin app port {} in the response",port);

            appSession.setMappedPort(port);
            appSession.setStatus("Alive");
            appSessionRepository.save(appSession);

        } else {
            //TODO: update the twin status as error?
            log.error("Twin extractor API call failed with status {} for instance {}" , responseEntity.getStatusCode(), appSession.getInstanceID());
        }

    }


    public String createTwinStream(JSONObject requestObject) throws JSONException {

        List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(requestObject.getString("twinVersionId"), "Available");
        String url = "dummy url";
        if(byTwinVersionIdAndStatus.isEmpty()){
            //Spawn new instance.
        }else{
            //TODO add partnersecure data and create the URL ex http://13.202.129.194:8011/streaming/webrtc-demo/?server=13.202.129.194
        }

        return url;
    }
}
