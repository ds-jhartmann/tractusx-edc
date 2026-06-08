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

package org.eclipse.tractusx.edc.dataplane.kafka;

import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DataFlowResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.junit.assertions.AbstractResultAssert;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclService;
import org.eclipse.tractusx.edc.dataplane.kafka.auth.KafkaOauthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.tractusx.edc.dataplane.kafka.KafkaBrokerDataFlowController.SECRET_NOT_DEFINED;
import static org.eclipse.tractusx.edc.dataplane.kafka.KafkaBrokerDataFlowController.START_FAILED;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.BOOTSTRAP_SERVERS;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.GROUP_PREFIX;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.KAFKA_TYPE;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.MECHANISM;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_ID;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_SECRET_KEY;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_REVOKE_URL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_TOKEN_URL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.POLL_DURATION;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.PROTOCOL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOKEN;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaBrokerDataFlowControllerTest {

    static final String NOT_DEFINED_SECRET_KEY = "clientSecretKey";
    static final String SECRET_KEY = "secret-key";
    // Intentionally distinct from the policy assignee so tests prove the kafka.group.prefix property is
    // honored rather than silently overridden by the assignee.
    static final String DATA_ADDRESS_GROUP_PREFIX = "provider-group-prefix";
    static final String ASSIGNEE = "consumer-bpn";

    final Vault vault = mock();
    final KafkaOauthService oauthService = mock();
    DataAddress contentDataAddress;
    TransferProcess transferProcess;
    Policy policy;

    @BeforeEach
    void setUp() {
        contentDataAddress = DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(TOPIC, "test-topic")
                .property(BOOTSTRAP_SERVERS, "localhost:9092")
                .property(PROTOCOL, "protocol")
                .property(MECHANISM, "mechanism")
                .property(GROUP_PREFIX, DATA_ADDRESS_GROUP_PREFIX)
                .property(POLL_DURATION, "PT5M")
                .property(OAUTH_TOKEN_URL, "http://localhost:8080/token")
                .property(OAUTH_REVOKE_URL, "http://keycloak:8080/revoke")
                .property(OAUTH_CLIENT_ID, "client-id")
                .property(OAUTH_CLIENT_SECRET_KEY, NOT_DEFINED_SECRET_KEY)
                .property(TOKEN, "token")
                .build();

        transferProcess = TransferProcess.Builder.newInstance()
                .contentDataAddress(contentDataAddress)
                .transferType("Kafka-PULL")
                .contractId("contract")
                .correlationId("correlation")
                .id("transferProcessId").build();
        policy = Policy.Builder.newInstance().assignee(ASSIGNEE).build();
    }

    private DataAddress contentDataAddressWithoutGroupPrefix() {
        return DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(TOPIC, "test-topic")
                .property(BOOTSTRAP_SERVERS, "localhost:9092")
                .property(PROTOCOL, "protocol")
                .property(MECHANISM, "mechanism")
                .property(POLL_DURATION, "PT5M")
                .property(OAUTH_TOKEN_URL, "http://localhost:8080/token")
                .property(OAUTH_REVOKE_URL, "http://keycloak:8080/revoke")
                .property(OAUTH_CLIENT_ID, "client-id")
                .property(OAUTH_CLIENT_SECRET_KEY, NOT_DEFINED_SECRET_KEY)
                .property(TOKEN, "token")
                .build();
    }

    @Nested
    class WithoutAcl {

        private KafkaBrokerDataFlowController controller;

        @BeforeEach
        void setUp() {
            lenient().when(oauthService.getAccessToken(any())).thenReturn("token");
            controller = new KafkaBrokerDataFlowController(vault, oauthService);
        }

        @Test
        void canHandle_ShouldReturnTrue_WhenTypeAndTransferTypeMatch() {
            assertThat(controller.canHandle(transferProcess)).isTrue();
        }

        @Test
        void canHandle_ShouldReturnFalse_WhenTypeDoesNotMatch() {
            contentDataAddress.setType("Non-Kafka");
            transferProcess.setContentDataAddress(contentDataAddress);
            assertThat(controller.canHandle(transferProcess)).isFalse();
        }

        @Test
        void canHandle_ShouldReturnFalse_WhenTransferTypeDoesNotMatch() {
            transferProcess = TransferProcess.Builder.newInstance().contentDataAddress(contentDataAddress).transferType("Not-Kafka-PULL").build();
            assertThat(controller.canHandle(transferProcess)).isFalse();
        }

        @Test
        void canHandle_ShouldReturnTrue_WhenContentDataAddressIsNull() {
            transferProcess = TransferProcess.Builder.newInstance().transferType("Kafka-PULL").build();
            assertThat(controller.canHandle(transferProcess)).isTrue();
        }

        @Test
        void prepare_ShouldReturnSuccess() {
            assertThat(controller.prepare(transferProcess, policy).succeeded()).isTrue();
        }

        @Test
        void transferTypesFor_ShouldReturnKafkaPull_WhenAssetIsKafkaBroker() {
            var asset = Asset.Builder.newInstance().id("asset-1")
                    .dataAddress(DataAddress.Builder.newInstance().type(KAFKA_TYPE).build())
                    .build();
            assertThat(controller.transferTypesFor(asset)).containsExactly("Kafka-PULL");
        }

        @Test
        void transferTypesFor_ShouldReturnEmpty_WhenAssetIsNotKafkaBroker() {
            var asset = Asset.Builder.newInstance().id("asset-1")
                    .dataAddress(DataAddress.Builder.newInstance().type("HttpData").build())
                    .build();
            assertThat(controller.transferTypesFor(asset)).isEmpty();
        }

        @Test
        void start_ShouldReturnSuccess_WhenValidInput() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);

            AbstractResultAssert.assertThat(controller.start(transferProcess, policy))
                    .isSucceeded()
                    .extracting(DataFlowResponse::getDataAddress)
                    .satisfies(addr -> {
                        assertThat(addr.getType()).isEqualTo(KAFKA_TYPE);
                        assertThat(addr.getStringProperty(BOOTSTRAP_SERVERS)).isEqualTo("localhost:9092");
                        assertThat(addr.getStringProperty(TOPIC)).isEqualTo("test-topic");
                        // The kafka.group.prefix property is honored over the assignee.
                        assertThat(addr.getStringProperty(GROUP_PREFIX)).isEqualTo(DATA_ADDRESS_GROUP_PREFIX);
                        assertThat(addr.getStringProperty(MECHANISM)).isEqualTo("mechanism");
                        assertThat(addr.getStringProperty(PROTOCOL)).isEqualTo("protocol");
                        assertThat(addr.getStringProperty(TOKEN)).isEqualTo("token");
                    });
        }

        @Test
        void start_ShouldFallBackToAssigneeGroupPrefix_WhenPropertyAbsent() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            transferProcess.setContentDataAddress(contentDataAddressWithoutGroupPrefix());

            AbstractResultAssert.assertThat(controller.start(transferProcess, policy))
                    .isSucceeded()
                    .extracting(DataFlowResponse::getDataAddress)
                    .satisfies(addr -> assertThat(addr.getStringProperty(GROUP_PREFIX)).isEqualTo(ASSIGNEE));
        }

        @Test
        void start_ShouldReturnFailure_WhenMissedSecret() {
            StatusResult<?> result = controller.start(transferProcess, policy);
            assertThat(result.fatalError()).isTrue();
            assertThat(result.getFailureDetail()).isEqualTo(START_FAILED + SECRET_NOT_DEFINED.formatted(NOT_DEFINED_SECRET_KEY));
        }

        @Test
        void suspend_ShouldReturnSuccess_WhenOperationSucceeds() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            assertThat(controller.suspend(transferProcess).succeeded()).isTrue();
        }

        @Test
        void suspend_ShouldReturnSuccess_WhenTokenAlreadyRemoved() {
            // No token stored for the transfer process: cleanup is idempotent and must not fail (e.g. a
            // suspended transfer that is later terminated, or a repeated suspend).
            StatusResult<?> result = controller.suspend(transferProcess);

            assertThat(result.succeeded()).isTrue();
            verify(oauthService, never()).revokeToken(any(), any());
            verify(vault, never()).deleteSecret(any());
        }

        @Test
        void terminate_ShouldReturnSuccess_WhenOperationSucceeds() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            assertThat(controller.terminate(transferProcess).succeeded()).isTrue();
        }

        @Test
        void terminate_ShouldReturnSuccess_WhenTokenAlreadyRemoved() {
            // Cleanup is idempotent: terminating a transfer whose token was already removed (e.g. after a
            // prior suspend) must not fail.
            StatusResult<?> result = controller.terminate(transferProcess);

            assertThat(result.succeeded()).isTrue();
            verify(oauthService, never()).revokeToken(any(), any());
            verify(vault, never()).deleteSecret(any());
        }
    }

    @Nested
    class WithAcl {

        private final KafkaAclService aclService = mock();
        private KafkaBrokerDataFlowController controller;

        @BeforeEach
        void setUp() {
            lenient().when(oauthService.getAccessToken(any())).thenReturn(validJwtWithSub("test-subject"));
            lenient().when(aclService.createAclsForSubject(any(), any(), any(), any())).thenReturn(Result.success());
            lenient().when(aclService.revokeAclsForTransferProcess(any())).thenReturn(Result.success());
            controller = new KafkaBrokerDataFlowController(vault, oauthService, aclService);
        }

        @Test
        void start_ShouldCreateAcls_WhenValidInput() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);

            StatusResult<DataFlowResponse> result = controller.start(transferProcess, policy);

            assertThat(result.succeeded()).isTrue();
            // The group prefix passed to ACL creation is the kafka.group.prefix property value (honored
            // over the assignee), and matches the prefix handed to the consumer in the EDR.
            verify(aclService).createAclsForSubject(eq("test-subject"), eq("test-topic"), eq(DATA_ADDRESS_GROUP_PREFIX), eq("transferProcessId"));
        }

        @Test
        void start_ShouldPassAssigneeAsGroupPrefix_ToAclCreation_WhenPropertyAbsent() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            transferProcess.setContentDataAddress(contentDataAddressWithoutGroupPrefix());

            StatusResult<DataFlowResponse> result = controller.start(transferProcess, policy);

            assertThat(result.succeeded()).isTrue();
            verify(aclService).createAclsForSubject(eq("test-subject"), eq("test-topic"), eq(ASSIGNEE), eq("transferProcessId"));
        }

        @Test
        void start_ShouldReturnFailure_WhenAclCreationFails() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            when(aclService.createAclsForSubject(any(), any(), any(), any())).thenReturn(Result.failure("ACL error"));

            StatusResult<DataFlowResponse> result = controller.start(transferProcess, policy);

            assertThat(result.fatalError()).isTrue();
            assertThat(result.getFailureDetail()).contains("Failed to create ACLs: ACL error");
            // The token stored before the ACL attempt is cleaned up so the vault retains nothing for a
            // transfer that never started.
            verify(vault).deleteSecret("transferProcessId");
        }

        @Test
        void start_ShouldReturnFailure_WhenTokenIsInvalidJwt() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            when(oauthService.getAccessToken(any())).thenReturn("not-a-jwt");

            StatusResult<DataFlowResponse> result = controller.start(transferProcess, policy);

            assertThat(result.fatalError()).isTrue();
            assertThat(result.getFailureDetail()).contains(START_FAILED);
            verify(aclService, never()).createAclsForSubject(anyString(), anyString(), anyString(), anyString());
        }

        @Test
        void suspend_ShouldRevokeAcls_BeforeTokenRevocation() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);

            StatusResult<?> result = controller.suspend(transferProcess);

            assertThat(result.succeeded()).isTrue();
            verify(aclService).revokeAclsForTransferProcess("transferProcessId");
        }

        @Test
        void suspend_ShouldReturnFailure_WhenAclRevocationFails() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);
            when(aclService.revokeAclsForTransferProcess(any())).thenReturn(Result.failure("ACL revoke error"));

            StatusResult<?> result = controller.suspend(transferProcess);

            assertThat(result.fatalError()).isTrue();
            assertThat(result.getFailureDetail()).contains("Failed to revoke ACLs: ACL revoke error");
        }

        @Test
        void terminate_ShouldRevokeAcls_BeforeTokenRevocation() {
            when(vault.resolveSecret(any())).thenReturn(SECRET_KEY);

            StatusResult<?> result = controller.terminate(transferProcess);

            assertThat(result.succeeded()).isTrue();
            verify(aclService).revokeAclsForTransferProcess("transferProcessId");
        }

        private String validJwtWithSub(String sub) {
            long now = Instant.now().getEpochSecond();
            String header = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes());
            String payload = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(("{\"sub\":\"" + sub + "\",\"iat\":" + now + ",\"exp\":" + (now + 300) + "}").getBytes());
            String sig = Base64.getUrlEncoder().withoutPadding().encodeToString("sig".getBytes());
            return header + "." + payload + "." + sig;
        }
    }
}
