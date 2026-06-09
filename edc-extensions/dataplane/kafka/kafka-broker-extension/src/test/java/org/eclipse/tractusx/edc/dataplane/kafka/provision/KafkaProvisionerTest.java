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

package org.eclipse.tractusx.edc.dataplane.kafka.provision;

import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionedResource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclService;
import org.eclipse.tractusx.edc.dataplane.kafka.auth.KafkaOauthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.BOOTSTRAP_SERVERS;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.GROUP_PREFIX;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.KAFKA_TYPE;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.MECHANISM;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_ID;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_CLIENT_SECRET_KEY;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.OAUTH_TOKEN_URL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.PROTOCOL;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOKEN;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.TOPIC;
import static org.eclipse.tractusx.edc.dataplane.kafka.provision.KafkaProvisionConstants.CONSUMER_GROUP_PREFIX_PROPERTY;
import static org.eclipse.tractusx.edc.dataplane.kafka.provision.KafkaProvisionConstants.KAFKA_RESOURCE_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProvisionerTest {

    private static final String FLOW_ID = "flow-1";
    private static final String SECRET_KEY = "client-secret-key";
    private static final String DATA_ADDRESS_GROUP_PREFIX = "provider-group-prefix";
    private static final String CONSUMER_BPN = "consumer-bpn";

    private final Vault vault = mock();
    private final KafkaOauthService oauthService = mock();
    private final KafkaAclService aclService = mock();
    private KafkaProvisioner provisioner;

    @BeforeEach
    void setUp() {
        provisioner = new KafkaProvisioner(vault, oauthService, aclService, mock(Monitor.class));
        when(vault.resolveSecret(SECRET_KEY)).thenReturn("secret-value");
        when(oauthService.getAccessToken(any())).thenReturn(jwtWithSub("kafka-subject"));
        when(aclService.createAclsForSubject(any(), any(), any(), any())).thenReturn(Result.success());
    }

    @Test
    void provision_mintsToken_createsAcls_andBuildsEdr() throws Exception {
        StatusResult<ProvisionedResource> result = provisioner.provision(resource(DATA_ADDRESS_GROUP_PREFIX)).get();

        assertThat(result.succeeded()).isTrue();
        DataAddress edr = result.getContent().getDataAddress();
        assertThat(edr.getType()).isEqualTo(KAFKA_TYPE);
        assertThat(edr.getStringProperty(TOPIC)).isEqualTo("test-topic");
        assertThat(edr.getStringProperty(TOKEN)).isEqualTo(jwtWithSub("kafka-subject"));
        // group prefix property is honored over the consumer BPN fallback
        assertThat(edr.getStringProperty(GROUP_PREFIX)).isEqualTo(DATA_ADDRESS_GROUP_PREFIX);

        verify(vault).storeSecret(eq(FLOW_ID), any());
        verify(aclService).createAclsForSubject("kafka-subject", "test-topic", DATA_ADDRESS_GROUP_PREFIX, FLOW_ID);
    }

    @Test
    void provision_fallsBackToConsumerBpn_whenGroupPrefixAbsent() throws Exception {
        StatusResult<ProvisionedResource> result = provisioner.provision(resource(null)).get();

        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent().getDataAddress().getStringProperty(GROUP_PREFIX)).isEqualTo(CONSUMER_BPN);
        verify(aclService).createAclsForSubject("kafka-subject", "test-topic", CONSUMER_BPN, FLOW_ID);
    }

    private ProvisionResource resource(String groupPrefix) {
        var builder = DataAddress.Builder.newInstance()
                .type(KAFKA_TYPE)
                .property(TOPIC, "test-topic")
                .property(BOOTSTRAP_SERVERS, "localhost:9092")
                .property(PROTOCOL, "SASL_PLAINTEXT")
                .property(MECHANISM, "OAUTHBEARER")
                .property(OAUTH_TOKEN_URL, "http://localhost:8080/token")
                .property(OAUTH_CLIENT_ID, "client-id")
                .property(OAUTH_CLIENT_SECRET_KEY, SECRET_KEY);
        if (groupPrefix != null) {
            builder.property(GROUP_PREFIX, groupPrefix);
        }
        return ProvisionResource.Builder.newInstance()
                .id("resource-1")
                .flowId(FLOW_ID)
                .type(KAFKA_RESOURCE_TYPE)
                .dataAddress(builder.build())
                .property(CONSUMER_GROUP_PREFIX_PROPERTY, CONSUMER_BPN)
                .build();
    }

    private static String jwtWithSub(String sub) {
        var enc = Base64.getUrlEncoder().withoutPadding();
        var header = enc.encodeToString("{\"alg\":\"none\"}".getBytes());
        var payload = enc.encodeToString(("{\"sub\":\"" + sub + "\"}").getBytes());
        var sig = enc.encodeToString("sig".getBytes());
        return header + "." + payload + "." + sig;
    }
}
