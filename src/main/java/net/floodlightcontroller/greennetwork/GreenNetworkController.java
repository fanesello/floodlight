package net.floodlightcontroller.greennetwork;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchManager;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.internal.OFSwitchManager;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.LinkInfo;
import net.floodlightcontroller.linkdiscovery.internal.LinkDiscoveryManager;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.ICMP;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.TopologyInstance;
import net.floodlightcontroller.topology.TopologyManager;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFMatchV3;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput.Builder;
import org.projectfloodlight.openflow.protocol.action.OFActionSetField;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.match.MatchFields;
import org.projectfloodlight.openflow.protocol.oxm.OFOxm;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.protocol.ver13.OFActionsVer13;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.ICMPv4Type;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author felipe.nesello
 *
 */
public class GreenNetworkController implements
		IFloodlightModule, IOFMessageListener, ILinkDiscoveryListener, IOFSwitchListener, ITopologyListener {
	
	protected IOFSwitchService swService;
	protected ITopologyService topoService;
	protected ILinkDiscoveryService linkDiscoverer;
	protected IFloodlightProviderService floodlightProvider;
	private TopologyManager topoManager;
	protected static Logger logger;

	@Override
	public String getName() {
		return GreenNetworkController.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		switch (msg.getType()) {
		case PACKET_IN:
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			logger.info("Enabled ports of {}: {}", sw.getId().toString(), sw.getEnabledPortNumbers().toString());

			if (eth.getEtherType() == EthType.IPv4) {
				IPv4 ip = (IPv4) eth.getPayload();
				
//				processOutputPacket(sw.getOFFactory(), eth);
//				buildFlowOnSwitch(sw);

				if (ip.getProtocol().equals(IpProtocol.ICMP)) {
					ICMP icmp = (ICMP) ip.getPayload();

					if (icmp.getIcmpType() == ICMP.ECHO_REQUEST) {
						logger.info("Ping detected: ICMP Request from host {} to {} in switch {}",
								ip.getSourceAddress().toString(), ip.getDestinationAddress().toString(), sw.getId().toString());
					} else if (icmp.getIcmpType() == ICMP.ECHO_REPLY) {
						logger.info("Ping detected: ICMP Reply from host {} to {} in switch {}",
								ip.getSourceAddress().toString(), ip.getDestinationAddress().toString(), sw.getId().toString());
					}
				}
			}
			break;
		default:
			break;
		}
		return Command.STOP;
	}
	
	private void printSwitches() {
		Set<DatapathId> dpIds = new HashSet<DatapathId>();
		
		dpIds = swService.getAllSwitchDpids();
		logger.info("{} switches found:", dpIds.size());
		for (DatapathId datapathId : dpIds) {
			logger.info(datapathId.toString());
		}
	}
	
	private void printLinks() {
		Map<Link, LinkInfo> links = linkDiscoverer.getLinks();
		logger.info("{} links found: {}", links.size(), links.toString());
	}

	private void removeSwitch() {
		IOFSwitch sw1 = swService.getSwitch(DatapathId.of("00:00:00:00:00:00:00:01"));
		if (sw1 != null) {
			logger.info("Removing switch {}...", sw1.getId().toString());
			
			topoManager.removeSwitch(sw1.getId());
			logger.info("Switch {} removed sucessfully.", sw1.getId().toString());
		}
	}

	private void buildFlowOnSwitch(IOFSwitch sw) {
		OFFactory factory = sw.getOFFactory();
		Match match = factory.buildMatch()
				.setExact(MatchField.IN_PORT, OFPort.of(1))
				.setExact(MatchField.ETH_TYPE, EthType.IPv4)
				.setExact(MatchField.IPV4_SRC, IPv4Address.of("10.0.0.1"))
				.setExact(MatchField.IP_PROTO, IpProtocol.ICMP)
				.setExact(MatchField.ICMPV4_TYPE, ICMPv4Type.ECHO)
				.build();
		
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = factory.actions();
//		OFOxms oxms = factory.oxms();
//		
//		OFActionSetField setDlDst = actions.buildSetField()
//			    .setField(
//			    		oxms.buildIpv4Dst()
//			    		.setValue(IPv4Address.of("10.0.0.1"))
//			    		.build()
//			    		)
//			        .build();
		
		OFActionOutput output = actions.buildOutput()
				.setPort(OFPort.of(2))
				.build();
		actionList.add(output);
		
		OFFlowAdd flowAdd = factory.buildFlowAdd()
			    .setBufferId(OFBufferId.NO_BUFFER)
			    .setHardTimeout(3600)
			    .setIdleTimeout(60)
			    .setPriority(32768)
			    .setMatch(match)
			    .setActions(actionList)
			    .setTableId(TableId.of(0))
			    .setOutPort(OFPort.of(2))
			    .build();
		
		sw.write(flowAdd);
	}

	private void processOutputPacket(IOFSwitch sw, Ethernet ethPacket) {
		
		if (sw.getId().equals(DatapathId.of("00:00:00:00:00:00:00:02"))) {

			//		OFActions actions = new OFActionsVer13();
			// Builder builder = actions.buildOutput();
			/* Specify the switch port(s) which the packet should be sent out. */
			OFActionOutput output = sw.getOFFactory().actions().buildOutput()
					.setPort(OFPort.of(2))
					.build();

			/* 
			 * Compose the OFPacketOut with the above Ethernet packet as the 
			 * payload/data, and the specified output port(s) as actions.
			 */
			OFPacketOut myPacketOut = sw.getOFFactory().buildPacketOut()
					.setData(ethPacket.serialize())
					.setBufferId(OFBufferId.NO_BUFFER)
					.setActions(Collections.singletonList((OFAction) output))
					.build();

			/* Write the packet to the switch via an IOFSwitch instance. */
			sw.write(myPacketOut);
		}

		//		OFPacketOut po = sw.getOFFactory().buildPacketOut()
		//			    .setActions(Collections.singletonList((OFAction) sw.getOFFactory().actions().output(OFPort.FLOOD, 0xffFFffFF)))
		//			    .setInPort(OFPort.CONTROLLER)
		//			    .build();
		//		sw.write(po);
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(ILinkDiscoveryService.class);
		l.add(ITopologyService.class);
		l.add(IOFSwitchService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		linkDiscoverer = context.getServiceImpl(ILinkDiscoveryService.class);
		topoService = context.getServiceImpl(ITopologyService.class);
		swService = context.getServiceImpl(IOFSwitchService.class);
		topoManager = context.getServiceImpl(TopologyManager.class);
	    logger = LoggerFactory.getLogger(GreenNetworkController.class);
    }

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		linkDiscoverer.addListener(this);
		topoService.addListener(this);
	}

	@Override
	public void switchAdded(DatapathId switchId) {
		logger.info("Switch {} added to the network.", switchId.toString());
	}

	@Override
	public void switchRemoved(DatapathId switchId) {
		logger.info("Switch {} removed from the network.", switchId.toString());
	}

	@Override
	public void switchActivated(DatapathId switchId) {
		logger.info("Switch {} activated on the network.", switchId.toString());
	}

	@Override
	public void switchPortChanged(DatapathId switchId, OFPortDesc port,
			PortChangeType type) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void switchChanged(DatapathId switchId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void linkDiscoveryUpdate(LDUpdate update) {
		logger.info("Link updated.");
	}

	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
		logger.info("Links updated.");
	}

	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates) {
		logger.info("Topology updated.");
		
		printSwitches();
		printLinks();
		removeSwitch();
	}

}
