/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.failover;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.RegistryLock;
import org.apache.dolphinscheduler.registry.api.utils.RegistryUtils;
import org.apache.dolphinscheduler.server.master.cluster.ClusterManager;
import org.apache.dolphinscheduler.server.master.cluster.MasterClusters;
import org.apache.dolphinscheduler.server.master.cluster.MasterServerMetadata;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.system.event.MasterFailoverEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FailoverCoordinatorTest {

    @InjectMocks
    private FailoverCoordinator failoverCoordinator;

    @Mock
    private RegistryClient registryClient;

    @Mock
    private ClusterManager clusterManager;

    @Mock
    private IWorkflowRepository workflowRepository;

    @Mock
    private TaskFailover taskFailover;

    @Mock
    private WorkflowInstanceDao workflowInstanceDao;

    @Mock
    private WorkflowFailover workflowFailover;

    @Mock
    private MasterClusters masterClusters;

    @Mock
    private RegistryLock registryLock;

    /**
     * Regression test for issue #18197: doMasterFailover used to acquire the lock at
     * {@code /lock/master-failover/<masterAddress>} but release {@code /lock/master-failover},
     * leaking the per-master lock. The fix uses a single lock-path local + try-with-resources.
     */
    @Test
    void failoverMaster_releasesTheSameLockItAcquired() {
        final String masterAddress = "127.0.0.1:5679";
        final MasterServerMetadata metadata = MasterServerMetadata.builder()
                .processId(1234)
                .serverStartupTime(System.currentTimeMillis())
                .address(masterAddress)
                .build();
        final MasterFailoverEvent event = MasterFailoverEvent.of(metadata, new Date(), 0L);

        when(clusterManager.getMasterClusters()).thenReturn(masterClusters);
        when(masterClusters.getServer(masterAddress)).thenReturn(Optional.empty());
        when(registryClient.getLock(anyString())).thenReturn(registryLock);
        when(registryClient.exists(anyString())).thenReturn(false);
        when(workflowInstanceDao.queryNeedFailoverWorkflowInstancesPaged(eq(masterAddress), any(), anyInt(), anyInt()))
                .thenReturn(Collections.emptyList());

        failoverCoordinator.failoverMaster(event);

        final ArgumentCaptor<String> lockPathCaptor = ArgumentCaptor.forClass(String.class);
        verify(registryClient).getLock(lockPathCaptor.capture());
        assertThat(lockPathCaptor.getValue())
                .isEqualTo(RegistryUtils.getMasterFailoverLockPath(masterAddress));
        verify(registryLock, times(1)).close();
    }

    /**
     * Verify that paginated failover processes multiple batches correctly.
     */
    @Test
    void failoverMaster_paginatedMultipleBatches() {
        final String masterAddress = "127.0.0.1:5679";
        final MasterServerMetadata metadata = MasterServerMetadata.builder()
                .processId(1234)
                .serverStartupTime(System.currentTimeMillis())
                .address(masterAddress)
                .build();
        final MasterFailoverEvent event = MasterFailoverEvent.of(metadata, new Date(), 0L);

        when(clusterManager.getMasterClusters()).thenReturn(masterClusters);
        when(masterClusters.getServer(masterAddress)).thenReturn(Optional.empty());
        when(registryClient.getLock(anyString())).thenReturn(registryLock);
        when(registryClient.exists(anyString())).thenReturn(false);

        // Create 3 workflow instances that need failover
        List<WorkflowInstance> batch1 = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            WorkflowInstance wi = new WorkflowInstance();
            wi.setId(i);
            wi.setStartTime(new Date(System.currentTimeMillis() - 60000));
            batch1.add(wi);
        }

        // First call returns 3 items (< batchSize=100), second call returns empty
        when(workflowInstanceDao.queryNeedFailoverWorkflowInstancesPaged(eq(masterAddress), any(), anyInt(), anyInt()))
                .thenReturn(batch1)
                .thenReturn(Collections.emptyList());

        when(workflowRepository.contains(anyInt())).thenReturn(false);

        failoverCoordinator.failoverMaster(event);

        // Should failover all 3 workflows
        verify(workflowFailover, times(3)).failoverWorkflow(any(WorkflowInstance.class));
        verify(registryLock, times(1)).close();
    }

    /**
     * Verify that workflows already in the repository are skipped, and no-progress break works.
     */
    @Test
    void failoverMaster_noProgressBreaks() {
        final String masterAddress = "127.0.0.1:5679";
        final MasterServerMetadata metadata = MasterServerMetadata.builder()
                .processId(1234)
                .serverStartupTime(System.currentTimeMillis())
                .address(masterAddress)
                .build();
        final MasterFailoverEvent event = MasterFailoverEvent.of(metadata, new Date(), 0L);

        when(clusterManager.getMasterClusters()).thenReturn(masterClusters);
        when(masterClusters.getServer(masterAddress)).thenReturn(Optional.empty());
        when(registryClient.getLock(anyString())).thenReturn(registryLock);
        when(registryClient.exists(anyString())).thenReturn(false);

        // All workflows are already in repository
        List<WorkflowInstance> batch = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            WorkflowInstance wi = new WorkflowInstance();
            wi.setId(i);
            wi.setStartTime(new Date(System.currentTimeMillis() - 60000));
            batch.add(wi);
        }

        when(workflowInstanceDao.queryNeedFailoverWorkflowInstancesPaged(eq(masterAddress), any(), anyInt(), anyInt()))
                .thenReturn(batch);
        when(workflowRepository.contains(anyInt())).thenReturn(true);

        failoverCoordinator.failoverMaster(event);

        // Should NOT failover any (all in repository), and should break without infinite loop
        verify(workflowFailover, times(0)).failoverWorkflow(any(WorkflowInstance.class));
        verify(registryLock, times(1)).close();
    }

    /**
     * Verify that a full batch (FAILOVER_BATCH_SIZE workflows) is processed and the loop continues
     * for the next batch. This exercises the pagination path without relying on fragile reflection
     * over compile-time inlined constants.
     */
    @Test
    void failoverMaster_fullBatchContinuesToNextBatch() {
        final String masterAddress = "127.0.0.1:5679";
        final MasterServerMetadata metadata = MasterServerMetadata.builder()
                .processId(1234)
                .serverStartupTime(System.currentTimeMillis())
                .address(masterAddress)
                .build();
        final MasterFailoverEvent event = MasterFailoverEvent.of(metadata, new Date(), 0L);

        when(clusterManager.getMasterClusters()).thenReturn(masterClusters);
        when(masterClusters.getServer(masterAddress)).thenReturn(Optional.empty());
        when(registryClient.getLock(anyString())).thenReturn(registryLock);
        when(registryClient.exists(anyString())).thenReturn(false);

        // Create exactly 100 workflows so the first batch is "full" and the loop continues.
        List<WorkflowInstance> fullBatch = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            WorkflowInstance wi = new WorkflowInstance();
            wi.setId(i);
            wi.setStartTime(new Date(System.currentTimeMillis() - 60000));
            fullBatch.add(wi);
        }

        when(workflowInstanceDao.queryNeedFailoverWorkflowInstancesPaged(eq(masterAddress), any(), anyInt(), anyInt()))
                .thenReturn(fullBatch)
                .thenReturn(Collections.emptyList());
        when(workflowRepository.contains(anyInt())).thenReturn(false);

        failoverCoordinator.failoverMaster(event);

        // Should query twice (full batch + empty follow-up) and failover all 100 workflows.
        verify(workflowInstanceDao, times(2))
                .queryNeedFailoverWorkflowInstancesPaged(eq(masterAddress), any(), anyInt(), anyInt());
        verify(workflowFailover, times(100)).failoverWorkflow(any(WorkflowInstance.class));
        verify(registryLock, times(1)).close();
    }
}
