package com.ursaleo.twin.scheduler.controller;

import com.ursaleo.twin.scheduler.Service.TwinHandlerService;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@Slf4j
public class TwinSchedulerController {


    @Autowired
    TwinHandlerService twinHandlerService;

    @GetMapping("/getTwinStream")
    public String getTwinStream(@RequestBody String requestBody){

        try {
            JSONObject requestObject = new JSONObject(requestBody);
            return twinHandlerService.createTwinStream(requestObject);

        } catch (JSONException e) {
            log.error("Error Parsing the request body : {}",requestBody);
        }

        return null;
    }
}
