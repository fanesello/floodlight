package net.floodlightcontroller.greennetwork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.forwarding.Forwarding;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.util.MatchUtils;
import net.floodlightcontroller.util.OFMessageDamper;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple packet processor, based on {@link Forwarding}.
 * @author felipe.nesello
 *
 */
@LogMessageCategory("Green Network Controller")
public class GNCPacketInProcessor {

	private static final int GNC_APP_ID = 16;

	private static final int FLOWMOD_DEFAULT_IDLE_TIMEOUT = 10;
	private static final int FLOWMOD_DEFAULT_HARD_TIMEOUT = 10;
	private static final int FLOWMOD_DEFAULT_PRIORITY = 1;

	private static int OFMESSAGE_DAMPER_CAPACITY = 10000;
	private static int OFMESSAGE_DAMPER_TIMEOUT = 250; // ms

	private static Logger logger = LoggerFactory.getLogger(GNCPacketInProcessor.class);

	private final IOFSwitchService switchService;
	private final ITopologyService topologyService;
	private final IRoutingService routingService;
	private final OFMessageDamper messageDamper;

	static {
		AppCookie.registerApp(GNC_APP_ID, "GreenNetworkController");
	}
	public static final U64 appCookie = AppCookie.makeCookie(GNC_APP_ID, 0);

	public GNCPacketInProcessor(IOFSwitchService swService, ITopologyService topoService, IRoutingService rtService) {
		this.switchService = swService;
		this.topologyService = topoService;
		this.routingService = rtService;
		this.messageDamper = new OFMessageDamper(OFMESSAGE_DAMPER_CAPACITY, EnumSet.of(OFType.FLOW_MOD), OFMESSAGE_DAMPER_TIMEOUT);
	}

	public boolean processPacket(IOFSwitch sw, FloodlightContext cntx, OFPacketIn packetIn) {
		Ethernet ethernetFrame = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		OFPort ingressPort = (packetIn.getVersion().compareTo(OFVersion.OF_12) < 0 ?
				packetIn.getInPort() : packetIn.getMatch().get(MatchField.IN_PORT));
		boolean hasProcessCompletedSuccessfully = false;

		if (ethernetFrame.isBroadcast()) {
			// If traffic is broadcast (ARP), flood to all ports
			hasProcessCompletedSuccessfully = processAsBroadcast(sw, packetIn, ingressPort);
		} else {
			// Else process packet as unicast
			hasProcessCompletedSuccessfully = processAsUnicast(sw, packetIn, ingressPort, cntx);
		}

		return hasProcessCompletedSuccessfully;
	}

	private boolean processAsUnicast(IOFSwitch sw, OFPacketIn packetIn, OFPort ingressPort, FloodlightContext cntx) {
		// Check if we the destination is reachable
		IDevice dstDevice = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE);

		if (dstDevice != null) {
			IDevice srcDevice = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);

			if (srcDevice != null) {
				if (!isIngressPortSameAsDestinationPort(sw, dstDevice, ingressPort)) {
					return addFlowsOnPathBetween(srcDevice, dstDevice, sw, ingressPort, cntx, packetIn);
				} else {
					logger.error("Both source and destination are on the same switch/port {}/{}, Action = NOP",
							sw.getId().toString(), ingressPort);
					return false;
				}
			} else {
				logger.error("No device entry found for source device");
				return false;
			}
		} else {
			// Treat as broadcast, because the destination was not found
			logger.info("Destination not found, flooding packet {}...", packetIn);
			return processAsBroadcast(sw, packetIn, ingressPort);
		}
	}

	private boolean addFlowsOnPathBetween(IDevice srcDevice, IDevice dstDevice, IOFSwitch sw, OFPort ingressPort,
			FloodlightContext cntx, OFPacketIn packetIn) {
		// assuming that every switch is connected to only one host
		// FIXME improve method to get 'N' hosts
		SwitchPort srcSwitchPort = srcDevice.getAttachmentPoints()[0];
		SwitchPort dstSwitchPort = dstDevice.getAttachmentPoints()[0];

		if (!srcSwitchPort.equals(dstSwitchPort)) {
			Route route = routingService.getRoute(
					srcSwitchPort.getSwitchDPID(), srcSwitchPort.getPort(),
					dstSwitchPort.getSwitchDPID(), dstSwitchPort.getPort(),
					U64.of(0));

			if (route != null) {
				Match match = createMatchFromPacket(sw, ingressPort, cntx);
				return pushRouteFlowOnAllHops(route, match, packetIn, sw.getId(), cntx, ingressPort);
			}
		}
		return false;
	}

	private boolean pushRouteFlowOnAllHops(Route route, Match match, OFPacketIn packetIn,
			DatapathId packetInSwitch, FloodlightContext cntx, OFPort ingressPort) {
		List<NodePortTuple> switchPortList = route.getPath();
		boolean hasPushedAllHops = true;

		// FIXME improve this iteration to iterate over switches, and not depending on indexes
		for (int indx = switchPortList.size() - 1; indx > 0; indx -= 2) {
			DatapathId switchDPID = switchPortList.get(indx).getNodeId();
			IOFSwitch sw = switchService.getSwitch(switchDPID);

			if (sw != null) {
				OFFactory factory = sw.getOFFactory();

				OFFlowMod.Builder flowBuilder = factory.buildFlowAdd();

				OFActionOutput.Builder actionOutputBuilder = factory.actions().buildOutput();
				List<OFAction> actions = new ArrayList<OFAction>();	
				Match.Builder matchBuilder = MatchUtils.createRetentiveBuilder(match);

				// set input and output ports on the switch
				OFPort outPort = switchPortList.get(indx).getPortId();
				OFPort inPort = switchPortList.get(indx - 1).getPortId();

				matchBuilder.setExact(MatchField.IN_PORT, inPort);
				actionOutputBuilder.setPort(outPort);
				actionOutputBuilder.setMaxLen(Integer.MAX_VALUE);
				actions.add(actionOutputBuilder.build());

				flowBuilder.setMatch(matchBuilder.build())
				.setActions(actions)
				.setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
				.setHardTimeout(FLOWMOD_DEFAULT_HARD_TIMEOUT)
				.setBufferId(OFBufferId.NO_BUFFER)
				.setCookie(appCookie)
				.setOutPort(outPort)
				.setPriority(FLOWMOD_DEFAULT_PRIORITY);

				try {
					if (logger.isTraceEnabled())
						logger.trace("Pushing route flowmod routeIndx={} sw={} inPort={} outPort={}",
								indx, sw, flowBuilder.getMatch().get(MatchField.IN_PORT), outPort);

					hasPushedAllHops &= messageDamper.write(sw, flowBuilder.build());

					if (isSourceSwitch(packetInSwitch, sw)) {
						// if it is the source switch, must forward the packet in
						// to the same output port of flow added
						sendPacketOut(sw, packetIn, outPort, cntx, ingressPort);
					}
				} catch (IOException e) {
					logger.error("Failure writing flow mod", e);
					hasPushedAllHops = false;
				}
			} else {
				logger.error("Switch with DPID {} not found", switchDPID.toString());
				hasPushedAllHops = false;
			}
		}

		return hasPushedAllHops;
	}

	private boolean isSourceSwitch(DatapathId packetInSwitch, IOFSwitch sw) {
		return sw.getId().equals(packetInSwitch);
	}

	private void sendPacketOut(IOFSwitch sw, OFPacketIn packetIn,
			OFPort outPort, FloodlightContext cntx, OFPort ingressPort) {
		if (!ingressPort.equals(outPort)) {
			// Send a packet out
			OFFactory factory = sw.getOFFactory();
			OFPacketOut.Builder packetOutbuilder = factory.buildPacketOut();

			List<OFAction> actions = new ArrayList<OFAction>();
			actions.add(factory.actions().output(outPort, Integer.MAX_VALUE));

			packetOutbuilder.setActions(actions);
			packetOutbuilder.setBufferId(OFBufferId.NO_BUFFER);
			packetOutbuilder.setData(packetIn.getData());
			packetOutbuilder.setInPort(ingressPort);

			try {
				messageDamper.write(sw, packetOutbuilder.build());
			} catch (IOException e) {
				logger.error("Failure writing packet out", e);
			}
		}
	}

	private Match createMatchFromPacket(IOFSwitch sw, OFPort ingressPort, FloodlightContext cntx) {
		Ethernet ethernetFrame = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		MacAddress srcMacAddress = ethernetFrame.getSourceMACAddress();
		MacAddress dstMacAddress = ethernetFrame.getDestinationMACAddress();

		Match.Builder matchBuilder = sw.getOFFactory().buildMatch();
		matchBuilder.setExact(MatchField.IN_PORT, ingressPort)
		.setExact(MatchField.ETH_SRC, srcMacAddress)
		.setExact(MatchField.ETH_DST, dstMacAddress);

		if (ethernetFrame.getEtherType() == EthType.IPv4) {
			IPv4 ipPacket = (IPv4) ethernetFrame.getPayload();
			IPv4Address srcIpAddress = ipPacket.getSourceAddress();
			IPv4Address dstIpAddress = ipPacket.getDestinationAddress();

			matchBuilder.setExact(MatchField.ETH_TYPE, EthType.IPv4)
			.setExact(MatchField.IPV4_SRC, srcIpAddress)
			.setExact(MatchField.IPV4_DST, dstIpAddress);
		} else if (ethernetFrame.getEtherType() == EthType.ARP) {
			matchBuilder.setExact(MatchField.ETH_TYPE, EthType.ARP);
		}

		return matchBuilder.build();
	}

	private boolean isIngressPortSameAsDestinationPort(IOFSwitch sw, IDevice dstDevice, OFPort ingressPort) {
		// Validate that the source and destination are not on the same switchport
		boolean isTheSamePort = false;

		for (SwitchPort dstAttachPoints : dstDevice.getAttachmentPoints()) {
			DatapathId dstSwitchDpid = dstAttachPoints.getSwitchDPID();

			if (isSourceSwitch(dstSwitchDpid, sw) && ingressPort.equals(dstAttachPoints.getPort())) {
				isTheSamePort = true;
			}
			break;
		}
		return isTheSamePort;
	}

	private boolean processAsBroadcast(IOFSwitch sw, OFPacketIn packetIn, OFPort ingressPort) {
		if (!topologyService.isIncomingBroadcastAllowed(sw.getId(), ingressPort)) {
			if (logger.isTraceEnabled())
				logger.trace("Broadcast not allowed on port {} of switch {}", ingressPort.toString(), sw.getId().toString());
			return false;
		}

		// Set Action to flood
		OFFactory factory = sw.getOFFactory();
		OFPacketOut.Builder packetOutBuilder = factory.buildPacketOut();
		List<OFAction> actions = new ArrayList<OFAction>();

		// If FLOOD port is supported, forward to flood instead of ALL
		if (sw.hasAttribute(IOFSwitch.PROP_SUPPORTS_OFPP_FLOOD)) {
			actions.add(factory.actions().output(OFPort.FLOOD, Integer.MAX_VALUE));
		} else {
			actions.add(factory.actions().output(OFPort.ALL, Integer.MAX_VALUE));
		}
		packetOutBuilder.setActions(actions);

		// set buffer-id, in-port and packet-data based on packet-in
		packetOutBuilder.setBufferId(OFBufferId.NO_BUFFER);
		packetOutBuilder.setInPort(ingressPort);
		packetOutBuilder.setData(packetIn.getData());

		try {
			if (logger.isTraceEnabled())
				logger.trace("Writing flood PacketOut switch={} packet-in={} packet-out={}", sw.getId().toString(), packetIn, packetOutBuilder.build());
			return messageDamper.write(sw, packetOutBuilder.build());
		} catch (IOException e) {
			logger.error("Failure writing PacketOut switch={} packet-in={} packet-out={}", sw.getId().toString(), packetIn, packetOutBuilder.build(), e);
		}

		return false;
	}

}
