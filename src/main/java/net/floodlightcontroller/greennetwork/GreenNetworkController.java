package net.floodlightcontroller.greennetwork;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.types.DatapathId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network controller that switches between a full topology routing (shortest path, Dijkstra) and
 * an energy saving topology routing (excluding unnecessary switches).
 * @author felipe.nesello
 *
 */
public class GreenNetworkController implements IFloodlightModule, IOFMessageListener, ITopologyListener {

	private static final int NETWORK_STATE_DELAY = 3;
	private static NetworkState networkState;

	protected static Logger logger = LoggerFactory.getLogger(GreenNetworkController.class);

	// FIXME Better way to set the switches, using hard coded for now
	static final Set<DatapathId> switchesToBeBlocked = new HashSet<DatapathId>(Arrays.asList(
			DatapathId.of("00:00:00:00:00:00:00:08"),
			DatapathId.of("00:00:00:00:00:00:00:09"),
			DatapathId.of("00:00:00:00:00:00:00:0a")
			));

	protected IThreadPoolService threadPoolService;
	protected IFloodlightProviderService floodlightProvider;
	protected IOFSwitchService switchService;
	protected ITopologyService topologyService;
	protected IRoutingService routingService;
	protected IDeviceService deviceService;

	private final GNCPacketInProcessor packetProcessor = new GNCPacketInProcessor(this);
	
	@Override
	public String getName() {
		return GreenNetworkController.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN) && (name.equals("topology") || name.equals("devicemanager")));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(ITopologyService.class);
		l.add(IOFSwitchService.class);
		l.add(IRoutingService.class);
		l.add(IDeviceService.class);
		l.add(IThreadPoolService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		topologyService = context.getServiceImpl(ITopologyService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);
		routingService = context.getServiceImpl(IRoutingService.class);
		deviceService = context.getServiceImpl(IDeviceService.class);
		threadPoolService = context.getServiceImpl(IThreadPoolService.class);
		logger = LoggerFactory.getLogger(GreenNetworkController.class);
		
		networkState = NetworkState.FULL_TOPOLOGY;
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		topologyService.addListener(this);
		
		ScheduledExecutorService scheduledExecutor = threadPoolService.getScheduledExecutor();
		
		final Runnable networkUpdater = new Runnable() {
			public void run() {
				changeNetworkState();
				printSwitches();
			}
		};
		final ScheduledFuture<?> beeperHandle = scheduledExecutor.scheduleAtFixedRate(networkUpdater, NETWORK_STATE_DELAY, NETWORK_STATE_DELAY, TimeUnit.MINUTES);
		
//		ses.schedule(new Runnable() {
//			public void run() { beeperHandle.cancel(true); }
//		}, 60, TimeUnit.SECONDS);
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		switch (msg.getType()) {
		case PACKET_IN:
			logger.info("Got a packet in message from switch {}.", sw.getId().toString());
			
			if (packetProcessor.processPacket(sw, cntx, (OFPacketIn) msg)) {
				logger.info("Packet processed successfully!");
			}
		default:
			break;
		}
		return Command.CONTINUE;
	}

	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates) {
		logger.info("Topology updated.");
		printSwitches();

		boolean energySaving = true;
		Set<DatapathId> switchesToBlock = new HashSet<DatapathId>();
		if (energySaving) {
			Set<DatapathId> allDpids = switchService.getAllSwitchDpids();

			for (DatapathId dpid : switchesToBeBlocked) {
				// Check if switch is present in topology, then add it to the block list
				if (allDpids.contains(dpid))
					switchesToBlock.add(dpid);
			}
		}
		topologyService.setSwitchesToBlock(switchesToBlock);
	}
	
	private void changeNetworkState() {
		
	}

	private void printSwitches() {
		Set<DatapathId> dpIds = new HashSet<DatapathId>();

		dpIds = switchService.getAllSwitchDpids();
		StringBuilder output = new StringBuilder(String.valueOf(dpIds.size()));

		output.append(" switches found:\n");
		for (DatapathId datapathId : dpIds) {
			output.append(datapathId.toString());
			output.append("\n");
		}
		logger.info(output.toString());
	}

}
