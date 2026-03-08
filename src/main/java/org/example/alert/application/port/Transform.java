package org.example.alert.application.port;

public interface Transform <I,O>{

    O transform(I input);
}
