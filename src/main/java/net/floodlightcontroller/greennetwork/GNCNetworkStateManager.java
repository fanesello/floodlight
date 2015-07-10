package net.floodlightcontroller.greennetwork;

import java.util.HashSet;
import java.util.Set;

import org.projectfloodlight.openflow.types.DatapathId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.topology.ITopologyService;

/**
 * Manages the network behavior to change between network states defined in {@link GNCNetworkState}.
 * @author felipe.nesello
 *
 */
public class GNCNetworkStateManager {
	
	private static final String WHITE_SPACE = " ";
	private static Logger logger = LoggerFactory.getLogger(GNCNetworkStateManager.class);
	
	private final IOFSwitchService switchService;
	private final ITopologyService topologyService;
	private final GNCPowerConsumptionCalculator powerCalculator;

	public GNCNetworkStateManager(IOFSwitchService swService, ITopologyService topoService) {
		this.switchService = swService;
		this.topologyService = topoService;
		this.powerCalculator = new GNCPowerConsumptionCalculator(swService);
	}
	
	public void changeState() {
		Set<DatapathId> switchesToBlock = new HashSet<DatapathId>();
		logger.info("Will change the network state");

		switch(GreenNetworkController.currentNetworkState) {
		case FULL_TOPOLOGY:
			// If current state is Full, change it to Green state and set the switches to block
			GreenNetworkController.currentNetworkState = GNCNetworkState.GREEN_TOPOLOGY;
			Set<DatapathId> allDpids = switchService.getAllSwitchDpids();

			for (DatapathId dpid : GreenNetworkController.switchesToBeBlocked) {
				// Check if switch is present in topology, then add it to the block list
				if (allDpids.contains(dpid))
					switchesToBlock.add(dpid);
			}
			break;
		case GREEN_TOPOLOGY:
		default:
			// If current state is Green, change it to Full state and do not block any switch
			// Default is to change the state to Full
			GreenNetworkController.currentNetworkState = GNCNetworkState.FULL_TOPOLOGY;
			break;
		}

		topologyService.setSwitchesToBlock(switchesToBlock);
		flushAllSwitches();
		printState();
	}

	private void printState() {
		StringBuilder outputString = new StringBuilder("Network state changed to")
		.append(WHITE_SPACE)
		.append(GreenNetworkController.currentNetworkState.toString())
		.append(WHITE_SPACE)
		.append("with a power consumption of")
		.append(WHITE_SPACE)
		.append(String.valueOf(powerCalculator.getCurrentNetworkPowerConsumption()))
		.append(WHITE_SPACE)
		.append("Wh");

		logger.info(outputString.toString());
	}

	private void flushAllSwitches() {
		for (DatapathId dpid : switchService.getAllSwitchDpids()) {
			IOFSwitch sw = switchService.getSwitch(dpid);
			sw.flush();
		}
	}
	
}