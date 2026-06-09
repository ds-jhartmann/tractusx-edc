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
import org.eclipse.edc.connector.dataplane.spi.edr.EndpointDataReferenceService;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.result.ServiceResult;
import org.eclipse.edc.spi.types.domain.DataAddress;

/**
 * Produces the consumer-facing EDR for a {@code KafkaBroker} PULL flow. The EDR is the {@link DataAddress}
 * built by {@link org.eclipse.tractusx.edc.dataplane.kafka.provision.KafkaProvisioner} (broker coordinates,
 * topic, security settings and the minted token) and surfaced on the data flow as its provisioned address.
 * Revocation of the broker ACLs and token is handled by the deprovisioner, so revoke here is a no-op.
 */
public class KafkaEndpointDataReferenceService implements EndpointDataReferenceService {

    @Override
    public Result<DataAddress> createEndpointDataReference(DataFlow dataFlow) {
        var edr = dataFlow.provisionedDataAddress();
        if (edr == null) {
            return Result.failure("No provisioned Kafka EDR available for data flow " + dataFlow.getId());
        }
        return Result.success(edr);
    }

    @Override
    public ServiceResult<Void> revokeEndpointDataReference(String transferProcessId, String reason) {
        return ServiceResult.success();
    }
}
