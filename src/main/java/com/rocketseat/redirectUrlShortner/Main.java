package com.rocketseat.redirectUrlShortner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        String pathParameters = (String) input.get("rawPath");
        String shortUrlCode = pathParameters != null ? pathParameters.replace("/", "") : null;

        if (shortUrlCode == null || shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid input: 'shortUrlCode' is required.");
        }

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("url--shortener--storage")
                .key(shortUrlCode + ".json")
                .build();

        InputStream s3ObjectStream = null;

        try {
            s3ObjectStream = s3Client.getObject(getObjectRequest);
        } catch (NoSuchKeyException e) {
            Map<String, Object> response = new HashMap<>();
            response.put("statusCode", 404);
            response.put("body", "URL not found.");
            context.getLogger().log("Error: URL not found for code: " + shortUrlCode);
            return response;
        } catch (Exception e) {
            context.getLogger().log("Error fetching data from S3: " + e.getMessage());
            Map<String, Object> response = new HashMap<>();
            response.put("statusCode", 500);
            response.put("body", "Internal Server Error: " + e.getMessage());
            return response;
        }

        UrlData urlData;

        try {
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);
        } catch (Exception e) {
            context.getLogger().log("Error deserializing URL data: " + e.getMessage());
            throw new RuntimeException("Error deserializing URL data: " + e.getMessage(), e);
        }

        long currentTimeInSeconds = System.currentTimeMillis() / 1000;

        Map<String, Object> response = new HashMap<>();

        // Cenário a URL expirou
        if (urlData.getExpirationTime() < currentTimeInSeconds) {
            response.put("statusCode", 410);
            response.put("body", "This URL has expired");
            return response;
        }

        // Cenário onde a URL ainda é válida
        response.put("statusCode", 302);
        Map<String, String> headers = new HashMap<>();
        headers.put("Location", urlData.getOriginalUrl());
        response.put("headers", headers);

        return response;
    }
}
