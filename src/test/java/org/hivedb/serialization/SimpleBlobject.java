package org.hivedb.serialization;

import java.util.Date;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.util.HiveUtils;
import org.hivedb.util.database.test.WeatherReportImpl;

public class SimpleBlobject extends Blobject {
	private Integer id;
	private String mapped;
	private Date notMapped;
	
	public SimpleBlobject(){}
	
	public SimpleBlobject(Integer id, String mapped, Date notMapped) {
		super();
		this.id = id;
		this.mapped = mapped;
		this.notMapped = notMapped;
	}

	@EntityId
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	// Just because ClassDaoServiceTest needs all of its entities within the same PartitionDImension
	@PartitionIndex
	public String getMapped() {
		return mapped;
	}
	public void setMapped(String mapped) {
		this.mapped = mapped;
	}
	public Date getNotMapped() {
		return notMapped;
	}
	public void setNotMapped(Date notMapped) {
		this.notMapped = notMapped;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		return hashCode() == obj.hashCode();
	}

	@Override
	public int hashCode() {
		return HiveUtils.makeHashCode(id, mapped, notMapped);
	}
}
