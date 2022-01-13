/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.misc;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class AKSKTokenGeneratorWithJWT {

    private static final String REQUEST = "request";
    private static final String ACCESS_KEY = "sub";
    private static final String SERVICE = "aud";
    private static final String DATE = "date";
    private static final String REGION = "region";
    private static final String ALGORITHM = "HmacSHA256";

    private final String secretKey;
    private final String accessKey;
    private final String date;
    private final String region;
    private final String service;

    public AKSKTokenGeneratorWithJWT(final String secretKey, final String accessKey, final String date, final String region, final String service) {
        this.secretKey = secretKey;
        this.accessKey = accessKey;
        this.date = date;
        this.region = region;
        this.service = service;
    }

    public String generate() {
        try {
            byte[] signingKey = signingKeyString();
            String token = signWithJWT(signingKey);
            return token;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private String signWithJWT(byte[] signingKey) throws Exception {
        Algorithm algorithmHS = Algorithm.HMAC256(signingKey);
        String token = JWT.create()
                .withClaim(ACCESS_KEY, this.accessKey)
                .withClaim(SERVICE, this.service)
                .withClaim(DATE, this.date)
                .withClaim(REGION, this.region)
                .sign(algorithmHS);
        return token;
    }

    private byte[] encode(byte[] key, byte[] data) throws Exception {
        Mac sha256HMAC = Mac.getInstance(ALGORITHM);
        SecretKeySpec secretKey = new SecretKeySpec(key, ALGORITHM);
        sha256HMAC.init(secretKey);
        return sha256HMAC.doFinal(data);
    }

    private byte[] signingKeyString() throws Exception {
        byte[] encoded = encode(this.secretKey.getBytes(), this.date.getBytes());
        encoded = encode(encoded, this.region.getBytes());
        encoded = encode(encoded, this.service.getBytes());
        encoded = encode(encoded, REQUEST.getBytes());
        return encoded;
    }
}
