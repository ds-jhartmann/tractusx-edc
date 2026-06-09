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

package org.eclipse.tractusx.edc.dataplane.kafka.flow;

import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.tractusx.edc.dataplane.kafka.acl.KafkaAclService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.tractusx.edc.dataplane.kafka.dataaddress.KafkaBrokerDataAddressSchema.KAFKA_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaEndpointDataReferenceServiceTest {

    private static final String FLOW_ID = "flow-1";

    private final KafkaAclService aclService = mock();
    private final KafkaEndpointDataReferenceService service = new KafkaEndpointDataReferenceService(aclService, mock(Monitor.class));

    @Test
    void createEndpointDataReference_returnsProvisionedAddress() {
        var address = DataAddress.Builder.newInstance().type(KAFKA_TYPE).build();
        var dataFlow = mock(DataFlow.class);
        when(dataFlow.provisionedDataAddress()).thenReturn(address);

        var result = service.createEndpointDataReference(dataFlow);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent()).isSameAs(address);
    }

    @Test
    void createEndpointDataReference_fails_whenNotProvisioned() {
        var dataFlow = mock(DataFlow.class);
        when(dataFlow.getId()).thenReturn(FLOW_ID);
        when(dataFlow.provisionedDataAddress()).thenReturn(null);

        assertThat(service.createEndpointDataReference(dataFlow).failed()).isTrue();
    }

    @Test
    void revoke_revokesAcls_onSuspendOrTerminate() {
        when(aclService.revokeAclsForTransferProcess(FLOW_ID)).thenReturn(Result.success());

        var result = service.revokeEndpointDataReference(FLOW_ID, "suspended");

        assertThat(result.succeeded()).isTrue();
        verify(aclService).revokeAclsForTransferProcess(FLOW_ID);
    }

    @Test
    void revoke_fails_whenAclRevocationFails() {
        when(aclService.revokeAclsForTransferProcess(FLOW_ID)).thenReturn(Result.failure("broker error"));

        assertThat(service.revokeEndpointDataReference(FLOW_ID, "terminated").failed()).isTrue();
    }

    @Test
    void revoke_isNoOp_whenAclManagementDisabled() {
        var noAclService = new KafkaEndpointDataReferenceService(null, mock(Monitor.class));

        var result = noAclService.revokeEndpointDataReference(FLOW_ID, "suspended");

        assertThat(result.succeeded()).isTrue();
        verifyNoInteractions(aclService);
    }
}
