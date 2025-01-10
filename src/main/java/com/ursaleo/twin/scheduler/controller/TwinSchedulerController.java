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

    @PostMapping("/shutdown")
public ResponseEntity<String> shutdownInstance(@RequestBody String requestBody) {
    try {
        // Parse the request body to a JSON object
        JSONObject requestObject = new JSONObject(requestBody);

        // Extract the public_ip from the request
        String publicIp = requestObject.getString("public_ip");
        int port = Integer.parseInt(requestObject.getString("port"));


        // Pass the public_ip to the twinHandlerService (assuming there's a method for this)
        // twinHandlerService.shutdownInstanceByPublicIp(publicIp);
        twinHandlerService.shutdownInstanceByPublicIpAndPort(publicIp, port);
        

        // Return success response
        return new ResponseEntity<>("Instance with public IP " + publicIp + " has been shut down.", HttpStatus.OK);

    } catch (JSONException e) {
        log.error("Error Parsing the request body : {}", requestBody);
        log.error("Error: {}", e.getMessage());
        return new ResponseEntity<>("Invalid JSON request body", HttpStatus.BAD_REQUEST);
    } catch (Exception e) {
        log.error("Error shutting down the instance for public_ip: {}", e.getMessage());
        return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}

}
