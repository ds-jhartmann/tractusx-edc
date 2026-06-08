/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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

package org.eclipse.tractusx.edc.dataplane.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DataFlowResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.ConsoleMonitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclService;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclServiceImpl;
import org.eclipse.tractusx.edc.dataplane.kafka.auth.KafkaOauthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Base64;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.BOOTSTRAP_SERVERS;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.GROUP_PREFIX;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.KAFKA_TYPE;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.MECHANISM;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_ID;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_SECRET_KEY;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_TOKEN_URL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.POLL_DURATION;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.PROTOCOL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOKEN;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Testcontainers
@EnabledIfSystemProperty(named = "testcontainers.enabled", matches = "true", disabledReason = "Testcontainers integration tests are disabled. Set -Dtestcontainers.enabled=true to enable them.")
class KafkaBrokerDataFlowControllerIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0")
            .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
            .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
            .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS");

    private final Vault vault = mock();
    private final KafkaOauthService oauthService = mock();

    private KafkaBrokerDataFlowController controller;
    private TransferProcess transferProcess;
    private Policy policy;

    @BeforeEach
    void setUp() {
        DataAddress contentDataAddress = DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(TOPIC, "integration-test-topic")
                .property(BOOTSTRAP_SERVERS, kafka.getBootstrapServers())
                .property(PROTOCOL, "SASL_PLAINTEXT")
                .property(MECHANISM, "OAUTHBEARER")
                .property(GROUP_PREFIX, "integration-group-prefix")
                .property(POLL_DURATION, "PT5M")
                .property(OAUTH_TOKEN_URL, "http://localhost:8080/token")
                .property(OAUTH_CLIENT_ID, "integration-client")
                .property(OAUTH_CLIENT_SECRET_KEY, "integration-secret-key")
                .property(TOKEN, "integration-token")
                .build();

        transferProcess = TransferProcess.Builder.newInstance()
                .contentDataAddress(contentDataAddress)
                .transferType("Kafka-PULL")
                .contractId("integration-contract")
                .correlationId("integration-correlation")
                .id("integration-transfer-process")
                .build();

        // Assignee deliberately differs from kafka.group.prefix so the EDR assertion proves the property
        // is honored rather than overridden by the assignee.
        policy = Policy.Builder.newInstance().assignee("integration-assignee").build();

        when(vault.resolveSecret("integration-secret-key")).thenReturn("integration-secret-value");
        when(vault.resolveSecret("test-secret-key")).thenReturn("test-secret-value");
        when(vault.resolveSecret("integration-transfer-process")).thenReturn("integration-oauth-token");
        when(oauthService.getAccessToken(any())).thenReturn(createValidJwtToken());

        Properties aclProperties = new Properties();
        aclProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        KafkaAclService aclService = new KafkaAclServiceImpl(aclProperties, new ConsoleMonitor());

        controller = new KafkaBrokerDataFlowController(vault, oauthService, aclService);
    }

    @Test
    void shouldStartTransferWithRealKafkaContainer() {
        StatusResult<DataFlowResponse> result = controller.start(transferProcess, policy);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent()).isNotNull();

        DataAddress resultDataAddress = result.getContent().getDataAddress();
        assertThat(resultDataAddress.getType()).isEqualTo(KAFKA_TYPE);
        assertThat(resultDataAddress.getStringProperty(BOOTSTRAP_SERVERS)).isEqualTo(kafka.getBootstrapServers());
        assertThat(resultDataAddress.getStringProperty(TOPIC)).isEqualTo("integration-test-topic");
        assertThat(resultDataAddress.getStringProperty(GROUP_PREFIX)).isEqualTo("integration-group-prefix");
    }

    @Test
    void shouldHandleKafkaUnavailability() {
        DataAddress invalidDataAddress = DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(TOPIC, "test-topic")
                .property(BOOTSTRAP_SERVERS, "invalid-kafka:9092")
                .property(PROTOCOL, "SASL_PLAINTEXT")
                .property(MECHANISM, "OAUTHBEARER")
                .property(GROUP_PREFIX, "test-group")
                .property(OAUTH_TOKEN_URL, "http://localhost:8080/token")
                .property(OAUTH_CLIENT_ID, "test-client")
                .property(OAUTH_CLIENT_SECRET_KEY, "test-secret-key")
                .build();

        TransferProcess invalidTransferProcess = TransferProcess.Builder.newInstance()
                .contentDataAddress(invalidDataAddress)
                .transferType("Kafka-PULL")
                .contractId("test-contract")
                .correlationId("test-correlation")
                .id("test-transfer-process")
                .build();

        // Controller does not validate Kafka connectivity at start, only OAuth token
        StatusResult<DataFlowResponse> result = controller.start(invalidTransferProcess, policy);

        assertThat(result.succeeded()).isTrue();
    }

    @Test
    void shouldSuspendTransferSuccessfully() {
        StatusResult<?> result = controller.suspend(transferProcess);

        assertThat(result.succeeded()).isTrue();
    }

    @Test
    void shouldTerminateTransferSuccessfully() {
        StatusResult<?> result = controller.terminate(transferProcess);

        assertThat(result.succeeded()).isTrue();
    }

    private String createValidJwtToken() {
        long now = Instant.now().getEpochSecond();
        long exp = now + 300;

        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes()
        );

        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                ("{\"sub\":\"test-subject\",\"iat\":" + now + ",\"exp\":" + exp + ",\"scope\":\"read write\"}").getBytes()
        );

        String signature = Base64.getUrlEncoder().withoutPadding().encodeToString("fake-signature".getBytes());

        return header + "." + payload + "." + signature;
    }
}
