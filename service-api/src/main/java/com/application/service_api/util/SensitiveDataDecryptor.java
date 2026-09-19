package com.application.service_api.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class SensitiveDataDecryptor {
    private SensitiveDataDecryptor() {
    }

    public static String decrypt(String encryptedValue) {
        if (encryptedValue == null || encryptedValue.isBlank()) {
            return encryptedValue;
        }
        return new String(Base64.getDecoder().decode(encryptedValue), StandardCharsets.UTF_8);
    }
}