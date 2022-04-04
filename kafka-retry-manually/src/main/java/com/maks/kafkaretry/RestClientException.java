package com.maks.kafkaretry;

public class RestClientException extends RuntimeException {

    public RestClientException(String message) {
        super(message);
    }
}
