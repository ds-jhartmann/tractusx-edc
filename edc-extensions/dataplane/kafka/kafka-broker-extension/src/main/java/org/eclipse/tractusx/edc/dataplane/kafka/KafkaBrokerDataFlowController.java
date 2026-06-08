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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.transfer.spi.flow.DataFlowController;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DataFlowResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclService;
import org.eclipse.tractusx.edc.dataplane.kafka.auth.KafkaOauthService;
import org.eclipse.tractusx.edc.dataplane.kafka.auth.OauthCredentials;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;

import static org.eclipse.edc.spi.response.ResponseStatus.FATAL_ERROR;
import static org.eclipse.edc.spi.types.domain.edr.EndpointDataReference.CONTRACT_ID;
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

/**
 * Implementation of the {@link DataFlowController} interface responsible for managing data flows
 * using Kafka.
 * It integrates with Oauth for token management and authorizes data transfer operations.
 * When {@code aclService} is provided, Kafka ACLs are created on start and revoked on
 * suspend/terminate, closing the window between termination and OAuth token expiry.
 */
class KafkaBrokerDataFlowController implements DataFlowController {
    public static final String DEFAULT_POLL_DURATION = Duration.ofSeconds(1).toString();
    static final String START_FAILED = "Failed to start data flow: ";
    static final String SUSPEND_FAILED = "Failed to suspend data flow: ";
    static final String TERMINATE_FAILED = "Failed to terminate data flow: ";
    static final String SECRET_NOT_DEFINED = "Secret key %s was not defined";
    private static final String TRANSFER_TYPE = "Kafka-PULL";

    private final Vault vault;
    private final KafkaOauthService oauthService;
    @Nullable
    private final KafkaAclService aclService;
    private final ObjectMapper objectMapper;

    KafkaBrokerDataFlowController(final Vault vault, final KafkaOauthService oauthService) {
        this(vault, oauthService, null, new ObjectMapper());
    }

    KafkaBrokerDataFlowController(final Vault vault, final KafkaOauthService oauthService, @Nullable final KafkaAclService aclService) {
        this(vault, oauthService, aclService, new ObjectMapper());
    }

    KafkaBrokerDataFlowController(final Vault vault, final KafkaOauthService oauthService, @Nullable final KafkaAclService aclService, final ObjectMapper objectMapper) {
        this.vault = vault;
        this.oauthService = oauthService;
        this.aclService = aclService;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean canHandle(final TransferProcess transferProcess) {
        var contentDataAddress = transferProcess.getContentDataAddress();
        if (contentDataAddress == null) {
            return TRANSFER_TYPE.equals(transferProcess.getTransferType());
        }
        return KAFKA_TYPE.equals(contentDataAddress.getType()) &&
                TRANSFER_TYPE.equals(transferProcess.getTransferType());
    }

    @Override
    public @NotNull StatusResult<DataFlowResponse> prepare(final TransferProcess transferProcess, final Policy policy) {
        // Kafka-PULL has no separate provisioning phase; provisioning happens during start().
        return StatusResult.success(DataFlowResponse.Builder.newInstance().build());
    }

    @Override
    public @NotNull StatusResult<DataFlowResponse> start(final TransferProcess transferProcess, final Policy policy) {
        var contentDataAddress = transferProcess.getContentDataAddress();

        String token;
        try {
            OauthCredentials creds = extractOauthCredentials(contentDataAddress);
            token = oauthService.getAccessToken(creds);
        } catch (Exception e) {
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, START_FAILED + e.getMessage());
        }
        vault.storeSecret(transferProcess.getId(), token);

        // Single source of truth for the consumer group prefix: honor the optional kafka.group.prefix
        // DataAddress property, falling back to the policy assignee (the consumer BPN) when absent. The
        // same value is handed to the consumer in the EDR and used to scope the broker GROUP ACL, so the
        // grant and the consumer instruction can never diverge.
        var groupPrefix = Optional.ofNullable(contentDataAddress.getStringProperty(GROUP_PREFIX))
                .orElse(policy.getAssignee());

        if (aclService != null) {
            try {
                String oauthSubject = extractOauthSubject(token);
                String topic = contentDataAddress.getStringProperty(TOPIC);
                var aclResult = aclService.createAclsForSubject(oauthSubject, topic, groupPrefix, transferProcess.getId());
                if (aclResult.failed()) {
                    vault.deleteSecret(transferProcess.getId());
                    return StatusResult.failure(FATAL_ERROR, START_FAILED + "Failed to create ACLs: " + aclResult.getFailureDetail());
                }
            } catch (EdcException e) {
                // Start failed after the token was stored; remove it so the vault does not retain a token
                // for a transfer that never started (the minted IdP token lapses at its TTL).
                vault.deleteSecret(transferProcess.getId());
                return StatusResult.failure(FATAL_ERROR, START_FAILED + e.getMessage());
            }
        }

        var pollDuration = Optional.ofNullable(contentDataAddress.getStringProperty(POLL_DURATION))
                .orElse(DEFAULT_POLL_DURATION);

        var endpointDataAddress = DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(BOOTSTRAP_SERVERS, contentDataAddress.getStringProperty(BOOTSTRAP_SERVERS))
                .property(TOPIC, contentDataAddress.getStringProperty(TOPIC))
                .property(PROTOCOL, contentDataAddress.getStringProperty(PROTOCOL))
                .property(MECHANISM, contentDataAddress.getStringProperty(MECHANISM))
                .property(TOKEN, token)
                .property(POLL_DURATION, pollDuration)
                .property(GROUP_PREFIX, groupPrefix)
                .property(CONTRACT_ID, transferProcess.getContractId())
                .build();

        return StatusResult.success(DataFlowResponse.Builder.newInstance()
                .dataAddress(endpointDataAddress)
                .build());
    }

    @Override
    public StatusResult<Void> suspend(final TransferProcess transferProcess) {
        return deleteCredentialsAndRevokeAccess(transferProcess, SUSPEND_FAILED);
    }

    @Override
    public StatusResult<Void> terminate(final TransferProcess transferProcess) {
        return deleteCredentialsAndRevokeAccess(transferProcess, TERMINATE_FAILED);
    }

    @Override
    public Set<String> transferTypesFor(final Asset asset) {
        var dataAddress = asset.getDataAddress();
        if (dataAddress != null && KAFKA_TYPE.equals(dataAddress.getType())) {
            return Set.of(TRANSFER_TYPE);
        }
        return Set.of();
    }

    private OauthCredentials extractOauthCredentials(final DataAddress contentDataAddress) {
        var tokenUrl = contentDataAddress.getStringProperty(OAUTH_TOKEN_URL);
        var revokeUrl = Optional.ofNullable(contentDataAddress.getStringProperty(OAUTH_REVOKE_URL));
        var clientId = contentDataAddress.getStringProperty(OAUTH_CLIENT_ID);
        var clientSecret = getSecret(contentDataAddress.getStringProperty(OAUTH_CLIENT_SECRET_KEY));

        return new OauthCredentials(tokenUrl, revokeUrl, clientId, clientSecret);
    }

    private String getSecret(final String secretKey) {
        return Optional.ofNullable(vault.resolveSecret(secretKey))
                .orElseThrow(() -> new EdcException(SECRET_NOT_DEFINED.formatted(secretKey)));
    }

    /**
     * Extracts the OAuth {@code sub} claim from a JWT token string.
     * The subject is used as the Kafka ACL principal when ACL management is enabled.
     */
    private String extractOauthSubject(String token) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) {
                throw new EdcException("Invalid JWT token format");
            }
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            var subNode = objectMapper.readTree(payload).get("sub");
            if (subNode == null || subNode.isNull()) {
                throw new EdcException("No 'sub' claim found in JWT token");
            }
            return subNode.asText();
        } catch (EdcException e) {
            throw e;
        } catch (Exception e) {
            throw new EdcException("Failed to extract OAuth subject from token: " + e.getMessage(), e);
        }
    }

    private StatusResult<Void> deleteCredentialsAndRevokeAccess(final TransferProcess transferProcess, final String error) {
        try {
            var transferProcessId = transferProcess.getId();

            if (aclService != null) {
                var aclResult = aclService.revokeAclsForTransferProcess(transferProcessId);
                if (aclResult.failed()) {
                    return StatusResult.failure(FATAL_ERROR, error + "Failed to revoke ACLs: " + aclResult.getFailureDetail());
                }
            }

            // Token cleanup is idempotent: a suspended transfer that is later terminated (or a repeated
            // suspend/terminate) will already have had its token removed, which is not an error.
            var token = vault.resolveSecret(transferProcessId);
            if (token != null) {
                var creds = extractOauthCredentials(transferProcess.getContentDataAddress());
                oauthService.revokeToken(creds, token);
                vault.deleteSecret(transferProcessId);
            }

            return StatusResult.success();

        } catch (Exception e) {
            return StatusResult.failure(FATAL_ERROR, error + e.getMessage());
        }
    }
}
