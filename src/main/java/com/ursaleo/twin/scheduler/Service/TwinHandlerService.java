package com.ursaleo.twin.scheduler.Service;

import com.ursaleo.twin.scheduler.model.AppSession;
import com.ursaleo.twin.scheduler.repository.AppSessionRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

@Service
@Slf4j
public class TwinHandlerService {

    @Value("${endpoints.extractor}")
    private String extractorEndpoint;

    @Autowired
    private AppSessionRepository appSessionRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    void invokeTwinHealthCheck(JSONArray instanceIds) {
               // TODO: call healthcheck lambda

        //for each returned health check do,
        AppSession appSession = new AppSession();
        appSession.setSessionId(UUID.randomUUID());
        appSession.setServerIP("192.168.0.1");
        appSession.setStatus("Alive");
        //appSession.setPartnerSecureData("secureData");
        appSession.setStartTime(LocalDateTime.now());
        //appSession.setEndTime(LocalDateTime.now().plusMinutes(1));

        invokeTwinExtractor(appSession);
    }

    private void invokeTwinExtractor(AppSession appSession) {

        URI uri = UriComponentsBuilder.fromHttpUrl(extractorEndpoint)
                .queryParam("appName", "java.exe")
                .build()
                .toUri();

        ResponseEntity<String> responseEntity = restTemplate.getForEntity(uri, String.class);
        try {
            JSONObject jsonObject = new JSONObject(responseEntity.getBody());
            log.info("Received extractor response : {}",jsonObject);

            appSession.setMappedPort(8080);
            appSession.setTwinVersionId("123456");
            appSessionRepository.save(appSession);

        } catch (JSONException e) {
            //TODO: update the twin status as error
            log.error("Autoscaling Request failed with status code: {}" , responseEntity.getStatusCode());
        }

    }


    public String createTwinStream(JSONObject requestObject) throws JSONException {

        List<AppSession> byTwinVersionIdAndStatus = appSessionRepository.findByTwinVersionIdAndStatus(requestObject.getString("twinVersionId"), "Available");
        String url = "";
        if(byTwinVersionIdAndStatus.isEmpty()){
            //Spawn new instance.
        }else{
            //TODO create the URL
        }

        return url;
    }
}
