package selfish;

import storm.utils.AbstractClientSpout;

public class ClientSpout extends AbstractClientSpout {
	private static final long serialVersionUID = 1L;

	@Override
	protected String getPullHost() {
		return "localhost";
	}

	@Override
	protected int getMaxPull() {
		return 10;
	}
}
