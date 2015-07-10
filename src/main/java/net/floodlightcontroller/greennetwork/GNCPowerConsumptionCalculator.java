package net.floodlightcontroller.greennetwork;

import java.util.HashSet;
import java.util.Set;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.internal.IOFSwitchService;

import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Power consumption calculator according to {@link GNCNetworkState}.
 * @author felipe.nesello
 *
 */
@LogMessageCategory("Green Network Controller")
public class GNCPowerConsumptionCalculator {

	private static final int PORT_CONSUMPTION = 5; // Wh
	private static final int SWITCH_CONSUMPTION = 50; // Wh

	private static Logger logger = LoggerFactory.getLogger(GNCPowerConsumptionCalculator.class);

	private final IOFSwitchService switchService;

	public GNCPowerConsumptionCalculator(IOFSwitchService swService) {
		this.switchService = swService;
	}

	public int getCurrentNetworkPowerConsumption() {
		int powerConsumption = 0;

		for (DatapathId dpid : switchService.getAllSwitchDpids()) {
			if (isInGreenStateAndSwitchIsBlocked(dpid)) {
				continue;
			}
			IOFSwitch sw = switchService.getSwitch(dpid);
			powerConsumption += SWITCH_CONSUMPTION;

			Set<OFPortDesc> portsToCalculate = new HashSet<OFPortDesc>();

			for (OFPortDesc port : sw.getEnabledPorts()) {
				if (!port.getPortNo().equals(OFPort.LOCAL))
					portsToCalculate.add(port);
			}
			if (logger.isTraceEnabled())
				logger.trace("Ports of switch {}: {}", sw.getSwitchDescription(), portsToCalculate);
			powerConsumption += portsToCalculate.size() * PORT_CONSUMPTION;
		}

		return powerConsumption;
	}

	private boolean isInGreenStateAndSwitchIsBlocked(DatapathId dpid) {
		return GreenNetworkController.currentNetworkState.equals(GNCNetworkState.GREEN_TOPOLOGY)
				&& GreenNetworkController.switchesToBeBlocked.contains(dpid);
	}

}
