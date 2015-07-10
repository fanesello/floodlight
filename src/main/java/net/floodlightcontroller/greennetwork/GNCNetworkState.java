package net.floodlightcontroller.greennetwork;

/**
 * Holds the possible states that the network can assume.
 * @author felipe.nesello
 *
 */
enum GNCNetworkState {

	FULL_TOPOLOGY("Full State"),
	GREEN_TOPOLOGY("Green State");
	
	private String name;
	
	GNCNetworkState(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		return this.name;
	}

}
