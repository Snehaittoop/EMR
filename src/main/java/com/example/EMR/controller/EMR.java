package com.example.EMR.controller;

import org.apache.kafka.common.protocol.types.Field;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping

public class EMR {
    @KafkaListener(topics = "telemetrysimulater",groupId = "group_id")
    public void Telemtrysimulatorconsumer(String message) {

        System.out.println("message = " + message);
    }

    private static final String Topic3="Telemetry";
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("observationtelemetry")
    public String processObservationTelemetry(@RequestBody String message) {
        JSONObject obj = new JSONObject(message);

        String temperatureaggr=obj.getString("temp");
        String  spo2aggr=obj.getString("spo2");
        String bpmaggr= obj.getString("bpm");
        String weightaggr=obj.getString("weight");
        String heightaggr=obj.getString("height");

        String var = "{temp:"+temperatureaggr+",spo2:"+spo2aggr+",bpm:"+bpmaggr+",weight:"+weightaggr+",height:"+heightaggr+"}";

        kafkaTemplate.send(Topic3,var);
        return "success";
    }
}
