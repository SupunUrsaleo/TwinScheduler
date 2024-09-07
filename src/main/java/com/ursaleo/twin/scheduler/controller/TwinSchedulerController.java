package com.ursaleo.twin.scheduler.controller;

import com.ursaleo.twin.scheduler.exception.TwinSchedulerException;
import com.ursaleo.twin.scheduler.service.TwinHandlerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.http.HttpResponse;

@RestController
@Slf4j
public class TwinSchedulerController {


    @Autowired
    TwinHandlerService twinHandlerService;

    @PostMapping("/getTwinStream")
    public ResponseEntity<String> getTwinStream(@RequestBody String requestBody){

        try {
            JSONObject requestObject = new JSONObject(requestBody);
            return new ResponseEntity<>(twinHandlerService.createTwinStream(requestObject), HttpStatus.OK);

        } catch (Exception e) {
            log.error("Error Parsing the request body : {}",requestBody);
            log.error("Error {}",e.getMessage());
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/releaseTwin/{publicIp}")
    public String releaseTwin(@PathVariable String publicIp){
        return twinHandlerService.releaseTwin(publicIp);
    }
}
