package org.hivedb.util.database.test;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Resource;
import org.hivedb.util.GeneratePrimitiveCollection;
import org.hivedb.util.HiveUtils;

@Resource("WeatherReport")
public class WeatherReportImpl implements WeatherReport {
	private static final String[] continents = 
		new String[]{"North America","South America", "Asia", "Europe","Africa","Australia","Antarctica"};
    private Integer reportId;
    private String continent;
    private BigDecimal latitude,longitude;
    private int temperature;
    private Date reportTime;
	public static final String REPORT_ID = "reportId";
	public static final String TEMPERATURE = "temperature";
	public static final String CONTINENT = "continent";
	
    @PartitionIndex(WeatherReport.CONTINENT)
	public String getContinent() {
		return continent;
	}
	public void setContinent(String continent) {
		this.continent = continent;
	}
	public BigDecimal getLatitude() {
		return latitude;
	}
	public void setLatitude(BigDecimal latitude) {
		this.latitude = latitude;
	}
	public BigDecimal getLongitude() {
		return longitude;
	}
	public void setLongitude(BigDecimal longitude) {
		this.longitude = longitude;
	}
	@EntityId
	public Integer getReportId() {
		return reportId;
	}
	public void setReportId(Integer reportId) {
		this.reportId = reportId;
	}
	
	public Date getReportTime() {
		return reportTime;
	}
	public void setReportTime(Date reportTime) {
		this.reportTime = reportTime;
	}
	@Index
	public int getTemperature() {
		return temperature;
	}
	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}
	
	private Collection<Integer> sources = new GeneratePrimitiveCollection<Integer>(Integer.class,7).generate();
	@Index
	public Collection<Integer> getSources() {
		return sources;
	}
	public void setSources(Collection<Integer> sources) {
		this.sources = sources;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.hashCode() == obj.hashCode();
	}
	@Override
	public int hashCode() {
		return HiveUtils.makeHashCode(new Object[]{getContinent(),getLatitude(), getLongitude(), getReportId(), getReportTime()});
	}
	public static WeatherReportImpl generate() {
		Random r = new Random(System.currentTimeMillis());
		WeatherReportImpl weatherReport = new WeatherReportImpl();
		weatherReport.setContinent(continents[r.nextInt(continents.length)]);
		weatherReport.setLatitude(new BigDecimal(360*r.nextDouble()));
		weatherReport.setLongitude(new BigDecimal(360*r.nextDouble()));
		weatherReport.setReportId(r.nextInt());
		weatherReport.setReportTime(new Date(System.currentTimeMillis()));
		weatherReport.setTemperature((int) Math.rint(Math.random() * 100));
		return weatherReport;
	}
	
	private Collection<WeatherEvent> weatherEvents;
	public Collection<WeatherEvent> getWeatherEvents() {
		return weatherEvents;
	}
	public void setWeatherEvents(Collection<WeatherEvent> weatherEvents) {
		this.weatherEvents = weatherEvents;
	}
	private int regionCode;
	public Integer getRegionCode() {
		return regionCode; 
	}
	public void setRegionCode(int regionCode) {
		this.regionCode = regionCode;
	}
}
