package org.hivedb.util.database.test;

import org.hivedb.annotations.EntityId;
import org.hivedb.annotations.Index;
import org.hivedb.annotations.PartitionIndex;
import org.hivedb.annotations.Resource;
import org.hivedb.util.ReflectionTools;
import org.hivedb.util.classgen.GenerateInstanceCollection;
import org.hivedb.util.classgen.GeneratePrimitiveCollection;
import org.hivedb.util.functional.Amass;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

@Resource("WeatherReport")
public class WeatherReportImpl implements WeatherReport {
	private static final String[] continents = 
		new String[]{"North America","South America", "Asia", "Europe","Africa","Australia","Antarctica"};
    private Integer reportId;
    private String continent;
    private Double latitude,longitude;
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
	public Double getLatitude() {
		return latitude;
	}
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	public Double getLongitude() {
		return longitude;
	}
	public void setLongitude(Double longitude) {
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
	static final Collection<Method> getters = ReflectionTools.getGetters(WeatherReport.class);
	@Override
	public int hashCode() {
		return Amass.makeHashCode(ReflectionTools.invokeGetters(this, getters));
	}
	public static WeatherReportImpl generate() {
		Random r = new Random(System.currentTimeMillis());
		WeatherReportImpl weatherReport = new WeatherReportImpl();
		weatherReport.setContinent(continents[r.nextInt(continents.length)]);
		weatherReport.setLatitude(new Double(360*r.nextDouble()));
		weatherReport.setLongitude(new Double(360*r.nextDouble()));
		weatherReport.setReportId(r.nextInt());
		weatherReport.setReportTime(new Date(System.currentTimeMillis()));
		weatherReport.setTemperature((int) Math.rint(Math.random() * 100));
		return weatherReport;
	}
	
	private Collection<WeatherEvent> weatherEvents = new GenerateInstanceCollection<WeatherEvent>(WeatherEvent.class,3).generate();
	@Index
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
	public void setRegionCode(Integer regionCode) {
		this.regionCode = regionCode;
	}
}
