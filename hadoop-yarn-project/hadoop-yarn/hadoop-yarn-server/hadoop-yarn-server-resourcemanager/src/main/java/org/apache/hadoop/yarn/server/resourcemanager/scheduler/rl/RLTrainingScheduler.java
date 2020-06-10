/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.rl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoAppAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class RLTrainingScheduler extends
    AbstractYarnScheduler<FifoAppAttempt, FiCaSchedulerNode> implements
    Configurable {

  private static final Logger LOG =
          LoggerFactory.getLogger(RLTrainingScheduler.class);

  private static final RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;

  private boolean usePortForNodeName;

  private ActiveUsersManager activeUsersManager;

  private static final String DEFAULT_QUEUE_NAME = "default";
  private QueueMetrics metrics;

  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  private final int resource_count = 2;
  private final int queue_size = 10;
  private final int feature_count = (resource_count + 1) * queue_size + 3 * resource_count + 1;
  // Feature Vector = [Count, CPU, Memory] * Queue Size +
  // [Total Remaining CPU Demands, Total Remaining Memory Demands] +
  // [Total Available CPU on Node, Total Available Memory on Node] +
  // [CPU Allocated due to prev action, Memory Allocated due to previous action] +
  // Previous Action

  private static final long DEFAULT_ASYNC_SCHEDULER_INTERVAL = 100;
  private AsyncScheduleThread asyncSchedulerThread;

  private float[] feature_vector = new float[feature_count];
  private List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
  private List<SchedulerRequestKey> schedulerRequestKeys = new ArrayList<SchedulerRequestKey>();

  private int previous_action;
  private Resource previous_action_allocation;
  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo(
            boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      queueInfo.setCapacity(1.0f);
      Resource clusterResource = getClusterResource();
      if (clusterResource.getMemorySize() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        queueInfo.setCurrentCapacity((float) usedResource.getMemorySize()
                / clusterResource.getMemorySize());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
              new HashMap<QueueACL, AccessControlList>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
            UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo =
              recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      return getQueueAcls().get(acl).isUserAllowed(user);
    }

    @Override
    public ActiveUsersManager getAbstractUsersManager() {
      return activeUsersManager;
    }

    @Override
    public void recoverContainer(Resource clusterResource,
                                 SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }
      increaseUsedResources(rmContainer);
      updateAppHeadRoom(schedulerAttempt);
      updateAvailableResourcesMetrics();
    }

    @Override
    public Set<String> getAccessibleNodeLabels() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public String getDefaultNodeLabelExpression() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    }

    @Override
    public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    }

    @Override
    public Priority getDefaultApplicationPriority() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }

    @Override
    public void decReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }
  };

  public RLTrainingScheduler() {
    super(RLTrainingScheduler.class.getName());
  }

  private synchronized void initScheduler(Configuration conf) {
    validateConf(conf);
    //Use ConcurrentSkipListMap because applications need to be ordered
    this.applications =
            new ConcurrentSkipListMap<>();
    this.minimumAllocation = super.getMinimumAllocation();
    initMaximumResourceCapability(super.getMaximumAllocation());
    this.usePortForNodeName = conf.getBoolean(
            YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false,
            conf);
    this.activeUsersManager = new ActiveUsersManager(metrics);

    asyncSchedulerThread = new AsyncScheduleThread(this);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);

    // Initialize SchedulingMonitorManager
    schedulingMonitorManager.initialize(rmContext, conf);
  }

  @Override
  public void serviceStart() throws Exception {
    assert (asyncSchedulerThread != null);
    asyncSchedulerThread.start();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
    assert (asyncSchedulerThread != null);
    asyncSchedulerThread.interrupt();
    asyncSchedulerThread.join(THREAD_JOIN_TIMEOUT_MS);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
              + " allocation configuration"
              + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
              + "=" + minMem
              + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
              + "=" + maxMem + ", min and max should be greater than 0"
              + ", max should be no smaller than min.");
    }
  }

  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void
  reinitialize(Configuration conf, RMContext rmContext) throws IOException {
    setConf(conf);
    super.reinitialize(conf, rmContext);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
                             List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
                             List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals,
                             ContainerUpdates updateRequests) {
    FifoAppAttempt application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed or non existent application " +
              applicationAttemptId.getApplicationId());
      return EMPTY_ALLOCATION;
    }

    // The allocate may be the leftover from previous attempt, and it will
    // impact current attempt, such as confuse the request and allocation for
    // current attempt's AM container.
    // Note outside precondition check for the attempt id may be
    // outdated here, so double check it here is necessary.
    if (!application.getApplicationAttemptId().equals(applicationAttemptId)) {
      LOG.error("Calling allocate on previous or removed " +
              "or non existent application attempt " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    normalizeResourceRequests(ask);

    // Release containers
    releaseContainers(release, application);

    synchronized (application) {

      // make sure we aren't stopping/removing the application
      // when the allocate comes in
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
                "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {
        LOG.debug("allocate: pre-update" +
                " applicationId=" + applicationAttemptId +
                " application=" + application);
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update" +
                " applicationId=" + applicationAttemptId +
                " application=" + application);
        application.showRequests();

        LOG.debug("allocate:" +
                " applicationId=" + applicationAttemptId +
                " #ask=" + ask.size());
      }

      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      Resource headroom = application.getHeadroom();
      application.setApplicationHeadroomForMetrics(headroom);
      return new Allocation(application.pullNewlyAllocatedContainers(),
              headroom, null, null, null, application.pullUpdatedNMTokens());
    }
  }

  @VisibleForTesting
  public synchronized void addApplication(ApplicationId applicationId,
                                          String queue, String user, boolean isAppRecovering) {
    SchedulerApplication<FifoAppAttempt> application =
            new SchedulerApplication<>(DEFAULT_QUEUE, user);
    applications.put(applicationId, application);
    metrics.submitApp(user);
    LOG.info("Accepted application " + applicationId + " from user: " + user
            + ", currently num of applications: " + applications.size());
    if (isAppRecovering) {
      LOG.debug("{} is recovering. Skip notifying APP_ACCEPTED",
              applicationId);
    } else {
      rmContext.getDispatcher().getEventHandler()
              .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
  }

  @VisibleForTesting
  public synchronized void
  addApplicationAttempt(ApplicationAttemptId appAttemptId,
                        boolean transferStateFromPreviousAttempt,
                        boolean isAttemptRecovering) {
    SchedulerApplication<FifoAppAttempt> application =
            applications.get(appAttemptId.getApplicationId());
    String user = application.getUser();
    // TODO: Fix store
    FifoAppAttempt schedulerApp =
            new FifoAppAttempt(appAttemptId, user, DEFAULT_QUEUE,
                    activeUsersManager, this.rmContext);

    if (transferStateFromPreviousAttempt) {
      schedulerApp.transferStateFromPreviousAttempt(application
              .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(schedulerApp);

    metrics.submitAppAttempt(user);
    LOG.info("Added Application Attempt " + appAttemptId
            + " to scheduler from user " + application.getUser());
    if (isAttemptRecovering) {
      LOG.debug("{} is recovering. Skipping notifying ATTEMPT_ADDED",
              appAttemptId);
    } else {
      rmContext.getDispatcher().getEventHandler().handle(
              new RMAppAttemptEvent(appAttemptId,
                      RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  private synchronized void doneApplication(ApplicationId applicationId,
                                            RMAppState finalState) {
    SchedulerApplication<FifoAppAttempt> application =
            applications.get(applicationId);
    if (application == null) {
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }

    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(application.getUser(),
            applicationId);
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void doneApplicationAttempt(
          ApplicationAttemptId applicationAttemptId,
          RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers)
          throws IOException {
    FifoAppAttempt attempt = getApplicationAttempt(applicationAttemptId);
    SchedulerApplication<FifoAppAttempt> application =
            applications.get(applicationAttemptId.getApplicationId());
    if (application == null || attempt == null) {
      throw new IOException("Unknown application " + applicationAttemptId +
              " has completed!");
    }

    // Kill all 'live' containers
    for (RMContainer container : attempt.getLiveContainers()) {
      if (keepContainers
              && container.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + container.getContainerId());
        continue;
      }
      super.completedContainer(container,
              SchedulerUtils.createAbnormalContainerStatus(
                      container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION),
              RMContainerEventType.KILL);
    }

    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);
  }

  private int getMaxAllocatableContainers(FifoAppAttempt application,
                                          SchedulerRequestKey schedulerKey, FiCaSchedulerNode node, NodeType type) {
    PendingAsk offswitchAsk = application.getPendingAsk(schedulerKey,
            ResourceRequest.ANY);
    int maxContainers = offswitchAsk.getCount();

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      PendingAsk rackLocalAsk = application.getPendingAsk(schedulerKey,
              node.getRackName());
      if (rackLocalAsk.getCount() <= 0) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers,
              rackLocalAsk.getCount());
    }

    if (type == NodeType.NODE_LOCAL) {
      PendingAsk nodeLocalAsk = application.getPendingAsk(schedulerKey,
              node.getRMNode().getHostName());

      if (nodeLocalAsk.getCount() > 0) {
        maxContainers = Math.min(maxContainers,
                nodeLocalAsk.getCount());
      }
    }

    return maxContainers;
  }


  private int assignContainersOnNode(FiCaSchedulerNode node,
                                     FifoAppAttempt application, SchedulerRequestKey schedulerKey
  ) {
    // Data-local
    int assignedContainers =
            assignNodeLocalContainers(node, application, schedulerKey);

    if (assignedContainers == 0) {
      // Rack-local
       assignedContainers =
              assignRackLocalContainers(node, application, schedulerKey);
    }
    if (assignedContainers == 0) {
      // Off-switch
      assignedContainers =
              assignOffSwitchContainers(node, application, schedulerKey);
    }

    LOG.debug("assignContainersOnNode:" +
            " node=" + node.getRMNode().getNodeAddress() +
            " application=" + application.getApplicationId().getId() +
            " priority=" + schedulerKey.getPriority() +
            " #assigned=" + assignedContainers);


    return assignedContainers;
  }

  private int assignNodeLocalContainers(FiCaSchedulerNode node,
                                        FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk nodeLocalAsk = application.getPendingAsk(schedulerKey,
            node.getNodeName());
    if (nodeLocalAsk.getCount() > 0) {
      // Don't allocate on this node if we don't need containers on this rack
      if (application.getOutstandingAsksCount(schedulerKey,
              node.getRackName()) <= 0) {
        return 0;
      }

      int assignableContainers = Math.min(
              getMaxAllocatableContainers(application, schedulerKey, node,
                      NodeType.NODE_LOCAL), nodeLocalAsk.getCount());
      assignableContainers = Math.min(assignableContainers, 1);
      assignedContainers =
              assignContainer(node, application, schedulerKey, assignableContainers,
                      nodeLocalAsk.getPerAllocationResource(), NodeType.NODE_LOCAL);
    }
    return assignedContainers;
  }

  private int assignRackLocalContainers(FiCaSchedulerNode node,
                                        FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk rackAsk = application.getPendingAsk(schedulerKey,
            node.getRMNode().getRackName());
    if (rackAsk.getCount() > 0) {
      // Don't allocate on this rack if the application doens't need containers
      if (application.getOutstandingAsksCount(schedulerKey,
              ResourceRequest.ANY) <= 0) {
        return 0;
      }

      int assignableContainers =
              Math.min(getMaxAllocatableContainers(application, schedulerKey, node,
                      NodeType.RACK_LOCAL), rackAsk.getCount());
      assignableContainers = Math.min(assignableContainers, 1);
      assignedContainers =
              assignContainer(node, application, schedulerKey, assignableContainers,
                      rackAsk.getPerAllocationResource(), NodeType.RACK_LOCAL);
    }
    return assignedContainers;
  }

  private int assignOffSwitchContainers(FiCaSchedulerNode node,
                                        FifoAppAttempt application, SchedulerRequestKey schedulerKey) {
    int assignedContainers = 0;
    PendingAsk offswitchAsk = application.getPendingAsk(schedulerKey,
            ResourceRequest.ANY);
    int assignableContainers = Math.min(offswitchAsk.getCount(), 1);

    if (offswitchAsk.getCount() > 0) {
      assignedContainers =
              assignContainer(node, application, schedulerKey,
                      assignableContainers,
                      offswitchAsk.getPerAllocationResource(), NodeType.OFF_SWITCH);
    }
    return assignedContainers;
  }

  private int assignContainer(FiCaSchedulerNode node, FifoAppAttempt application,
                              SchedulerRequestKey schedulerKey, int assignableContainers,
                              Resource capability, NodeType type) {
    LOG.debug("assignContainers:" +
            " node=" + node.getRMNode().getNodeAddress() +
            " application=" + application.getApplicationId().getId() +
            " priority=" + schedulerKey.getPriority().getPriority() +
            " assignableContainers=" + assignableContainers +
            " capability=" + capability + " type=" + type);

    // TODO: A buggy application with this zero would crash the scheduler.
    int availableContainers =
            (int) (node.getUnallocatedResource().getMemorySize() /
                    capability.getMemorySize());
    int assignedContainers =
            Math.min(assignableContainers, availableContainers);

    if (assignedContainers > 0) {
      for (int i = 0; i < assignedContainers; ++i) {

        NodeId nodeId = node.getRMNode().getNodeID();
        ContainerId containerId = BuilderUtils.newContainerId(application
                .getApplicationAttemptId(), application.getNewContainerId());

        // Create the container
        Container container = BuilderUtils.newContainer(containerId, nodeId,
                node.getRMNode().getHttpAddress(), capability,
                schedulerKey.getPriority(), null,
                schedulerKey.getAllocationRequestId());

        // Allocate!

        // Inform the application
        RMContainer rmContainer = application.allocate(type, node, schedulerKey,
                container);

        // Inform the node
        node.allocateContainer(rmContainer);

        // Update usage for this container
        increaseUsedResources(rmContainer);
        previous_action_allocation = rmContainer.getAllocatedResource();
      }

    }

    return assignedContainers;
  }

  private void increaseUsedResources(RMContainer rmContainer) {
    Resources.addTo(usedResource, rmContainer.getAllocatedResource());
  }

  private void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
    schedulerAttempt.setHeadroom(Resources.subtract(getClusterResource(),
            usedResource));
  }

  private void updateAvailableResourcesMetrics() {
    metrics.setAvailableResourcesToQueue(
            Resources.subtract(getClusterResource(), usedResource));
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
      case NODE_ADDED: {
        NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
        addNode(nodeAddedEvent.getAddedRMNode());
        recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
                nodeAddedEvent.getAddedRMNode());

      }
      break;
      case NODE_REMOVED: {
        NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
        removeNode(nodeRemovedEvent.getRemovedRMNode());
      }
      break;
      case NODE_RESOURCE_UPDATE: {
        NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
                (NodeResourceUpdateSchedulerEvent) event;
        updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
                nodeResourceUpdatedEvent.getResourceOption());
      }
      break;
      case NODE_UPDATE: {
        NodeUpdateSchedulerEvent nodeUpdatedEvent =
                (NodeUpdateSchedulerEvent) event;
        nodeUpdate(nodeUpdatedEvent.getRMNode());
      }
      break;
      case APP_ADDED: {
        AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
        addApplication(appAddedEvent.getApplicationId(),
                appAddedEvent.getQueue(), appAddedEvent.getUser(),
                appAddedEvent.getIsAppRecovering());
      }
      break;
      case APP_REMOVED: {
        AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
        doneApplication(appRemovedEvent.getApplicationID(),
                appRemovedEvent.getFinalState());
      }
      break;
      case APP_ATTEMPT_ADDED: {
        AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
                (AppAttemptAddedSchedulerEvent) event;
        addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
                appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
                appAttemptAddedEvent.getIsAttemptRecovering());
      }
      break;
      case APP_ATTEMPT_REMOVED: {
        AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
                (AppAttemptRemovedSchedulerEvent) event;
        try {
          doneApplicationAttempt(
                  appAttemptRemovedEvent.getApplicationAttemptID(),
                  appAttemptRemovedEvent.getFinalAttemptState(),
                  appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
        } catch (IOException ie) {
          LOG.error("Unable to remove application "
                  + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
        }
      }
      break;
      case CONTAINER_EXPIRED: {
        ContainerExpiredSchedulerEvent containerExpiredEvent =
                (ContainerExpiredSchedulerEvent) event;
        ContainerId containerid = containerExpiredEvent.getContainerId();
        super.completedContainer(getRMContainer(containerid),
                SchedulerUtils.createAbnormalContainerStatus(
                        containerid,
                        SchedulerUtils.EXPIRED_CONTAINER),
                RMContainerEventType.EXPIRE);
      }
      break;
      case RELEASE_CONTAINER: {
        if (!(event instanceof ReleaseContainerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        RMContainer container = ((ReleaseContainerEvent) event).getContainer();
        completedContainer(container,
                SchedulerUtils.createAbnormalContainerStatus(
                        container.getContainerId(),
                        SchedulerUtils.RELEASED_CONTAINER),
                RMContainerEventType.RELEASED);
      }
      break;
      default:
        LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  @Lock(RLTrainingScheduler.class)
  @Override
  protected synchronized void completedContainerInternal(
          RMContainer rmContainer, ContainerStatus containerStatus,
          RMContainerEventType event) {

    // Get the application for the finished container
    Container container = rmContainer.getContainer();
    FifoAppAttempt application =
            getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
            container.getId().getApplicationAttemptId().getApplicationId();

    // Get the node on which the container was allocated
    FiCaSchedulerNode node = (FiCaSchedulerNode) getNode(container.getNodeId());

    if (application == null) {
      LOG.info("Unknown application: " + appId +
              " released container " + container.getId() +
              " on node: " + node +
              " with event: " + event);
      return;
    }

    // Inform the application
    application.containerCompleted(rmContainer, containerStatus, event,
            RMNodeLabelsManager.NO_LABEL);

    // Inform the node
    node.releaseContainer(rmContainer.getContainerId(), false);

    // Update total usage
    Resources.subtractFrom(usedResource, container.getResource());

    LOG.info("Application attempt " + application.getApplicationAttemptId() +
            " released container " + container.getId() +
            " on node: " + node +
            " with event: " + event);

  }

  private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

  private synchronized void removeNode(RMNode nodeInfo) {
    FiCaSchedulerNode node = nodeTracker.getNode(nodeInfo.getNodeID());
    if (node == null) {
      return;
    }
    // Kill running containers
    for (RMContainer container : node.getCopiedListOfRunningContainers()) {
      super.completedContainer(container,
              SchedulerUtils.createAbnormalContainerStatus(
                      container.getContainerId(),
                      SchedulerUtils.LOST_CONTAINER),
              RMContainerEventType.KILL);
    }
    nodeTracker.removeNode(nodeInfo.getNodeID());
  }

  @Override
  public QueueInfo getQueueInfo(String queueName,
                                boolean includeChildQueues, boolean recursive) {
    return DEFAULT_QUEUE.getQueueInfo(false, false);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return DEFAULT_QUEUE.getQueueUserAclInfo(null);
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  private synchronized void addNode(RMNode nodeManager) {
    FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
            usePortForNodeName);
    nodeTracker.addNode(schedulerNode);

    if (getNumClusterNodes() == 1) {
      asyncSchedulerThread.beginSchedule();
    }
  }

  @Override
  public void recover(RMState state) {
    // NOT IMPLEMENTED
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FifoAppAttempt attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return DEFAULT_QUEUE.getMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
                                          QueueACL acl, String queueName) {
    return DEFAULT_QUEUE.hasAccess(acl, callerUGI);
  }

  @Override
  public synchronized List<ApplicationAttemptId>
  getAppsInQueue(String queueName) {
    if (queueName.equals(DEFAULT_QUEUE.getQueueName())) {
      List<ApplicationAttemptId> attempts =
              new ArrayList<ApplicationAttemptId>(applications.size());
      for (SchedulerApplication<FifoAppAttempt> app : applications.values()) {
        attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
      }
      return attempts;
    } else {
      return null;
    }
  }

  public Resource getUsedResource() {
    return usedResource;
  }

  @Override
  protected synchronized void nodeUpdate(RMNode nm) {
    super.nodeUpdate(nm);

    FiCaSchedulerNode node = (FiCaSchedulerNode) getNode(nm.getNodeID());
    if (rmContext.isWorkPreservingRecoveryEnabled()
            && !rmContext.isSchedulerReadyForAllocatingContainers()) {
      return;
    }

    // A decommissioned node might be removed before we get here
    if (node != null &&
            Resources.greaterThanOrEqual(resourceCalculator, getClusterResource(),
                    node.getUnallocatedResource(), minimumAllocation)) {
      LOG.debug("Node heartbeat " + nm.getNodeID() +
              " available resource = " + node.getUnallocatedResource());

      // assignContainers(node);

      LOG.debug("Node after allocation " + nm.getNodeID() + " resource = "
              + node.getUnallocatedResource());
    }

    updateAvailableResourcesMetrics();
  }

  @VisibleForTesting
  @Override
  public void killContainer(RMContainer container) {
    ContainerStatus status = SchedulerUtils.createKilledContainerStatus(
            container.getContainerId(),
            "Killed by RM to simulate an AM container failure");
    LOG.info("Killing container " + container);
    completedContainer(container, status, RMContainerEventType.KILL);
  }

  @Override
  public synchronized void recoverContainersOnNode(
          List<NMContainerStatus> containerReports, RMNode nm) {
    super.recoverContainersOnNode(containerReports, nm);
  }

  static class AsyncScheduleThread extends Thread {
    private final RLTrainingScheduler scheduler;
    private AtomicBoolean runSchedules = new AtomicBoolean(false);
    private final ZContext zContext;
    private ZMQ.Socket socket;
    private long previous = 0;

    public AsyncScheduleThread(RLTrainingScheduler scheduler) {
      this.scheduler = scheduler;
      this.zContext = new ZContext();
      setDaemon(true);
    }

    public void beginSchedule() {
      socket = zContext.createSocket(SocketType.REQ);
      String port = System.getenv("ZMQ_SERVER_PORT");
      if (port == null || port.isEmpty()) {
        port = "5555";
      }
      String address = "tcp://localhost:" + port;
      socket.connect(address);
      runSchedules.set(true);
    }

    public void suspendSchedule() {
      runSchedules.set(false);
    }

    @Override
    public void run() {
      int debuggingLogCounter = 0;
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!runSchedules.get()) {
            Thread.sleep(100);
          } else {
            boolean jobsWaiting = scheduler.updateQueueState();
            boolean jobsExecuting = scheduler.updateNodeState();
            long time = System.currentTimeMillis();
            long reward = (previous != 0) ? (previous - time) : 0;
            previous = (jobsWaiting || jobsExecuting) ? time : 0;

            String features = scheduler.getInputFeatures();
            LOG.info("Feature Vector: " + features + "\tReward: " + reward);
            String request = features + " : " + reward;
            socket.send(request);
            String resp = socket.recvStr();
            int action = Integer.valueOf(resp);
            LOG.info("Got Action: " + action);
            boolean validAction = scheduler.takeAction(action);
            if (!validAction) {
              Thread.sleep(scheduler.getAsyncScheduleInterval());
            }
            if (LOG.isDebugEnabled()) {
              // Adding a debug log here to ensure that the thread is alive
              // and running fine.
              if (debuggingLogCounter++ > 10000) {
                debuggingLogCounter = 0;
                LOG.debug("AsyncScheduleThread[" + getName() + "] is running!");
              }
            }
          }
        } catch (InterruptedException ie) {
          // keep interrupt signal
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  long getAsyncScheduleInterval() {
    return DEFAULT_ASYNC_SCHEDULER_INTERVAL;
  }

  // Returns true if there are any pending demands
  // Returns false if there are no pending demands
  boolean updateQueueState() {
    int queue_entries_done = 0;
    int offset = 0;
    float pendingVirtualCores = 0;
    float pendingMemory = 0;
    boolean pendingAllocations = false;
    applicationIds.clear();
    schedulerRequestKeys.clear();
    for (Map.Entry<ApplicationId, SchedulerApplication<FifoAppAttempt>> e : applications
            .entrySet()) {
      FifoAppAttempt application = e.getValue().getCurrentAppAttempt();
      if (application == null) {
        continue;
      }

      synchronized (application) {
        // Check if this resource is on the blacklist
        // Ignoring this constraint for now
        // if (SchedulerAppUtils.isPlaceBlacklisted(application, node, LOG)) {
        //  continue;
        // }
        for (SchedulerRequestKey schedulerKey :
                application.getSchedulerKeys()) {
          PendingAsk pendingAsk = application.getPendingAsk(schedulerKey,
                  ResourceRequest.ANY);

          int count = pendingAsk.getCount();
          if (count == 0) {
            continue;
          }
          pendingAllocations = true;
          Resource resource = pendingAsk.getPerAllocationResource();
          float cores = ((float) resource.getVirtualCores())
                  / getMinimumAllocation().getVirtualCores();
          float memory = ((float) resource.getMemorySize())
                  / getMinimumAllocation().getMemorySize();
          if (queue_entries_done < queue_size) {
            feature_vector[offset++] = count;
            feature_vector[offset++] = cores;
            feature_vector[offset++] = memory;
            applicationIds.add(e.getKey());
            schedulerRequestKeys.add(schedulerKey);
            queue_entries_done++;
          } else {
            pendingVirtualCores += count * cores;
            pendingMemory += count * memory;
          }
        }
      }
    }
    while (queue_entries_done < queue_size) {
      feature_vector[offset++] = 0;
      feature_vector[offset++] = 0;
      feature_vector[offset++] = 0;
      queue_entries_done++;
    }
    feature_vector[offset++] = pendingVirtualCores;
    feature_vector[offset++] = pendingMemory;
    return pendingAllocations;
  }

  // Returns true if some resources on the node are currently used
  // Returns false otherwise
  boolean updateNodeState() {
    List<FiCaSchedulerNode> nodes = nodeTracker.getAllNodes();
    assert (nodes.size() <= 1);
    boolean resourcesAllocated = false;
    int offset = (resource_count + 1) * queue_size + resource_count;
    if (nodes.size() == 0) {
      feature_vector[offset++] = 0;
      feature_vector[offset++] = 0;
    } else {
      // Should only be one node in the collection for now
      FiCaSchedulerNode node = nodes.get(0);
      feature_vector[offset++] = ((float) node.getUnallocatedResource().getVirtualCores())
              / getMinimumAllocation().getVirtualCores();
      feature_vector[offset++] = ((float) node.getUnallocatedResource().getMemorySize())
              / getMinimumAllocation().getMemorySize();

      if (Resources.lessThan(resourceCalculator, getClusterResource(),
              node.getUnallocatedResource(), node.getTotalResource())) {
        resourcesAllocated = true;
      }
    }
    feature_vector[offset++] = ((float) previous_action_allocation.getVirtualCores())
            / getMinimumAllocation().getVirtualCores();
    feature_vector[offset++] = ((float) previous_action_allocation.getMemorySize())
            / getMinimumAllocation().getMemorySize();
    return resourcesAllocated;
  }

  String getInputFeatures() {
    return Arrays.toString(feature_vector);
  }

  boolean skipSchedulingThisRound() {
    FiCaSchedulerNode node = nodeTracker.getAllNodes().get(0);
    if (Resources.lessThan(resourceCalculator, getClusterResource(),
            node.getUnallocatedResource(), minimumAllocation)) {
      return true;
    }
    return false;
  }

  /* Returns true if some action was taken
  *  Else returns false */
  boolean takeAction(int action) throws InterruptedException {
    previous_action = action;

    boolean retVal = false;
    FiCaSchedulerNode node = nodeTracker.getAllNodes().get(0);
    if (nodeTracker.getAllNodes().size() == 0) {
      LOG.debug("No active nodes. Returning!");
    } else
    if (action >= queue_size) {
      LOG.debug("Selected NOOP action. Returning!");
      return retVal;
    } else if ((action >= applicationIds.size()) || (action >= schedulerRequestKeys.size())) {
      LOG.debug("Selected null queue element. Returning!");
    } else {
      ApplicationId applicationId = applicationIds.get(action);
      assert (applications.containsKey(applicationId));
      FifoAppAttempt application = applications.get(applicationId).getCurrentAppAttempt();
      assert (application != null);
      // Flaw with current approach: It is possible that the same function
      // executing on another node might have already allocated containers on this
      SchedulerRequestKey schedulerKey = schedulerRequestKeys.get(action);
      int assignedContainers =
              assignContainersOnNode(node, application, schedulerKey);

      if (assignedContainers > 0) {
        assert (assignedContainers == 1);
        retVal = true;
      }

      // Update the applications' headroom to correctly take into
      // account the containers assigned in this update.
      updateAppHeadRoom(application);
      LOG.debug("Allocated " + assignedContainers);
    }
    if (!retVal) {
      // Nothing was allocated in the previous iteration
      previous_action_allocation.setVirtualCores(0);
      previous_action_allocation.setMemorySize(0);
    }
    return retVal;
  }
}

