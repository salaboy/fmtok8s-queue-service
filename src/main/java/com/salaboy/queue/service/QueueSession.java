package com.salaboy.queue.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueSession {
    private String sessionId;
    private String clientId;

    public QueueSession() {
    }

    public QueueSession(String sessionId, String clientId) {
        this.sessionId = sessionId;
        this.clientId = clientId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return "TicketPurchaseSession{" +
                "sessionId='" + sessionId + '\'' +
                ", clientId='" + clientId + '\'' +
                '}';
    }
}
