package net.floodlightcontroller.greennetwork;

import java.util.ArrayList;
import java.util.List;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.topology.NodePortTuple;

import org.apache.commons.lang3.StringUtils;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.U64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors the network state stability.
 * @author felipe.nesello
 *
 */
@LogMessageCategory("Green Network Controller")
public class GNCNetworkStateMonitor {

	private static final DatapathId SOURCE_SWITCH = DatapathId.of("00:00:00:00:00:00:00:01");
	private static final DatapathId DEST_SWITCH = DatapathId.of("00:00:00:00:00:00:00:04");

	private static final String LINE_BREAK = "\n";
	private static final String WHITE_SPACE = " ";

	private static Logger logger = LoggerFactory.getLogger(GNCNetworkStateMonitor.class);

	private final IOFSwitchService switchService;
	private final IRoutingService routingService;

	public GNCNetworkStateMonitor(IOFSwitchService swService, IRoutingService rtService) {
		this.switchService = swService;
		this.routingService = rtService;
	}

	public void monitorState() {
		IOFSwitch srcSwitch = switchService.getSwitch(SOURCE_SWITCH);
		IOFSwitch dstSwitch = switchService.getSwitch(DEST_SWITCH);

		Route route = routingService.getRoute(srcSwitch.getId(), dstSwitch.getId(), U64.of(0));
		if (route != null) {
			List<DatapathId> dpidsOnRoute = extractDpidsFromRoute(route);
			String stabilityResult = (isNetworkStable(dpidsOnRoute)) ? "stable" : "unstable";

			StringBuilder outputString = new StringBuilder("Route information from")
			.append(WHITE_SPACE)
			.append(srcSwitch.getId().toString())
			.append(WHITE_SPACE)
			.append("to")
			.append(WHITE_SPACE)
			.append(dstSwitch.getId().toString())
			.append(":")
			.append(LINE_BREAK)
			.append(StringUtils.join(dpidsOnRoute, LINE_BREAK))
			.append(LINE_BREAK)
			.append("Network route for")
			.append(WHITE_SPACE)
			.append(GreenNetworkController.currentNetworkState.toString())
			.append(WHITE_SPACE)
			.append("is")
			.append(WHITE_SPACE)
			.append(stabilityResult);

			logger.info(outputString.toString());
		} else {
			logger.error("Could not get route from {} to {}", srcSwitch.getId().toString(), dstSwitch.getId().toString());
		}
	}

	private boolean isNetworkStable(List<DatapathId> dpidsOnRoute) {
		boolean isStable = false;

		for (DatapathId dpidOnPath : dpidsOnRoute) {
			if (GreenNetworkController.currentNetworkState.equals(GNCNetworkState.FULL_TOPOLOGY)) {
				if (GreenNetworkController.switchesToBeBlocked.contains(dpidOnPath)) {
					isStable = true;
					break;
				}
			} else {
				if (GreenNetworkController.switchesToBeBlocked.contains(dpidOnPath)) {
					isStable = false;
					break;
				} else {
					isStable = true;
				}
			}
		}

		return isStable;
	}

	private List<DatapathId> extractDpidsFromRoute(Route route) {
		List<DatapathId> dpidsOnPath = new ArrayList<DatapathId>();

		for (NodePortTuple nodePort : route.getPath()) {
			if (!dpidsOnPath.contains(nodePort.getNodeId())) {
				dpidsOnPath.add(nodePort.getNodeId());
			}
		}

		return dpidsOnPath;
	}

}
