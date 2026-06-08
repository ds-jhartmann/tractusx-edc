/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.eclipse.tractusx.edc.tests.transfer;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import jakarta.json.Json;
import org.eclipse.edc.jsonld.spi.JsonLd;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.tractusx.edc.tests.kafka.KafkaExtension;
import org.eclipse.tractusx.edc.tests.participant.TransferParticipant;
import org.eclipse.tractusx.edc.tests.runtimes.PostgresExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.STARTED;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.TYPE;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.CONSUMER_BPN;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.CONSUMER_DID;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.CONSUMER_NAME;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.DSP_2025;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.DSP_2025_PATH;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.PROVIDER_BPN;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.PROVIDER_DID;
import static org.eclipse.tractusx.edc.tests.TestRuntimeConfiguration.PROVIDER_NAME;
import static org.eclipse.tractusx.edc.tests.helpers.PolicyHelperFunctions.bpnPolicy;
import static org.eclipse.tractusx.edc.tests.participant.TractusxParticipantBase.ASYNC_TIMEOUT;
import static org.eclipse.tractusx.edc.tests.runtimes.Runtimes.pgRuntime;

/**
 * End-to-end test for the Kafka-PULL transfer type. Runs a real Kafka broker via Testcontainers
 * and a WireMock-backed OAuth2 token endpoint.
 */
@EndToEndTest
public class KafkaPullEndToEndTest {

    private static final String TOPIC = "test-topic";
    private static final String CLIENT_SECRET_KEY = "kafka-client-secret";

    private static final TransferParticipant CONSUMER = TransferParticipant.Builder.newInstance()
            .name(CONSUMER_NAME)
            .id(CONSUMER_DID)
            .bpn(CONSUMER_BPN)
            .protocol(DSP_2025)
            .protocolVersionPath(DSP_2025_PATH)
            .build();

    private static final TransferParticipant PROVIDER = TransferParticipant.Builder.newInstance()
            .name(PROVIDER_NAME)
            .id(PROVIDER_DID)
            .bpn(PROVIDER_BPN)
            .protocol(DSP_2025)
            .protocolVersionPath(DSP_2025_PATH)
            .build();

    @RegisterExtension
    @Order(0)
    private static final PostgresExtension POSTGRES = new PostgresExtension(PROVIDER.getName(), CONSUMER.getName());

    @RegisterExtension
    private static final RuntimeExtension PROVIDER_RUNTIME = pgRuntime(PROVIDER, POSTGRES, PROVIDER::getConfig);

    @RegisterExtension
    private static final RuntimeExtension CONSUMER_RUNTIME = pgRuntime(CONSUMER, POSTGRES, CONSUMER::getConfig);

    @RegisterExtension
    private static final KafkaExtension KAFKA = new KafkaExtension();

    @RegisterExtension
    private static final WireMockExtension OAUTH = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    @BeforeAll
    static void beforeAll() {
        CONSUMER.setJsonLd(CONSUMER_RUNTIME.getService(JsonLd.class));
    }

    @Test
    void kafkaPullTransfer_consumerReceivesMessages() {
        OAUTH.stubFor(post(urlEqualTo("/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"test-access-token\",\"expires_in\":3600}")));
        OAUTH.stubFor(post(urlEqualTo("/revoke"))
                .willReturn(aResponse().withStatus(200)));

        PROVIDER_RUNTIME.getService(Vault.class).storeSecret(CLIENT_SECRET_KEY, "kafka-client-secret-value");

        KAFKA.createTopic(TOPIC);
        KAFKA.produce(TOPIC, "k1", "hello");
        KAFKA.produce(TOPIC, "k2", "world");

        var assetId = "kafka-test-asset";
        var dataAddress = Map.<String, Object>of(
                "name", "kafka-transfer-test",
                "@type", "DataAddress",
                "type", "KafkaBroker",
                "topic", TOPIC,
                "kafka.bootstrap.servers", KAFKA.getBootstrapServers(),
                "kafka.security.protocol", "SASL_PLAINTEXT",
                "kafka.sasl.mechanism", "OAUTHBEARER",
                "kafka.group.prefix", CONSUMER.getBpn(),
                "kafka.poll.duration", "PT1S",
                "tokenUrl", OAUTH.baseUrl() + "/token"
        );
        var dataAddressFull = new java.util.HashMap<>(dataAddress);
        dataAddressFull.put("revokeUrl", OAUTH.baseUrl() + "/revoke");
        dataAddressFull.put("clientId", "kafka-client-id");
        dataAddressFull.put("clientSecretKey", CLIENT_SECRET_KEY);

        PROVIDER.createAsset(assetId, Map.of(), dataAddressFull);
        var policyId = PROVIDER.createPolicyDefinition(bpnPolicy(CONSUMER.getBpn()));
        PROVIDER.createContractDefinition(assetId, "def-1", policyId, policyId);

        var destination = Json.createObjectBuilder()
                .add(TYPE, EDC_NAMESPACE + "DataAddress")
                .add(EDC_NAMESPACE + "type", "HttpData")
                .add(EDC_NAMESPACE + "baseUrl", "http://placeholder")
                .build();

        var transferProcessId = CONSUMER
                .requestAssetFrom(assetId, PROVIDER)
                .withTransferType("Kafka-PULL")
                .withDestination(destination)
                .execute();

        CONSUMER.waitForTransferProcess(transferProcessId, STARTED);

        var consumed = KAFKA.consume(TOPIC, Duration.ofSeconds(10));
        assertThat(consumed).isNotEmpty();

        OAUTH.verify(postRequestedFor(urlEqualTo("/token")));
    }

    @Test
    void kafkaPullTransfer_suspendRevokesToken() {
        OAUTH.stubFor(post(urlEqualTo("/token"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"access_token\":\"test-access-token\",\"expires_in\":3600}")));
        OAUTH.stubFor(post(urlEqualTo("/revoke"))
                .willReturn(aResponse().withStatus(200)));

        PROVIDER_RUNTIME.getService(Vault.class).storeSecret(CLIENT_SECRET_KEY, "kafka-client-secret-value");

        var assetId = "kafka-suspend-asset";
        var dataAddress = new java.util.HashMap<String, Object>();
        dataAddress.put("name", "kafka-suspend-test");
        dataAddress.put("@type", "DataAddress");
        dataAddress.put("type", "KafkaBroker");
        dataAddress.put("topic", TOPIC);
        dataAddress.put("kafka.bootstrap.servers", KAFKA.getBootstrapServers());
        dataAddress.put("kafka.security.protocol", "SASL_PLAINTEXT");
        dataAddress.put("kafka.sasl.mechanism", "OAUTHBEARER");
        dataAddress.put("tokenUrl", OAUTH.baseUrl() + "/token");
        dataAddress.put("revokeUrl", OAUTH.baseUrl() + "/revoke");
        dataAddress.put("clientId", "kafka-client-id");
        dataAddress.put("clientSecretKey", CLIENT_SECRET_KEY);

        PROVIDER.createAsset(assetId, Map.of(), dataAddress);
        var policyId = PROVIDER.createPolicyDefinition(bpnPolicy(CONSUMER.getBpn()));
        PROVIDER.createContractDefinition(assetId, "def-2", policyId, policyId);

        var destination = Json.createObjectBuilder()
                .add(TYPE, EDC_NAMESPACE + "DataAddress")
                .add(EDC_NAMESPACE + "type", "HttpData")
                .add(EDC_NAMESPACE + "baseUrl", "http://placeholder")
                .build();

        var transferProcessId = CONSUMER
                .requestAssetFrom(assetId, PROVIDER)
                .withTransferType("Kafka-PULL")
                .withDestination(destination)
                .execute();

        CONSUMER.waitForTransferProcess(transferProcessId, STARTED);

        await().atMost(ASYNC_TIMEOUT).untilAsserted(() ->
                OAUTH.verify(postRequestedFor(urlEqualTo("/token"))));
    }
}
