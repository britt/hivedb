package org.hivedb.management;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

public class FillStatisticsMBeanImpl extends StandardMBean implements
		FillStatisticsMBean {
	private long capacity;
	private long fill;

	public FillStatisticsMBeanImpl() throws NotCompliantMBeanException {
		super(FillStatisticsMBean.class);
	}

	public FillStatisticsMBeanImpl(Class arg0)
			throws NotCompliantMBeanException {
		super(arg0);
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public long getFill() {
		return fill;
	}

	public void setFill(long fill) {
		this.fill = fill;
	}

	public float getPercentage() {
		return getFill() / getCapacity();
	}	
}
