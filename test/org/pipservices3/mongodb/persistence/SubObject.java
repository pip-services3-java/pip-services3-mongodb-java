package org.pipservices3.mongodb.persistence;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SubObject {
	private String _type;
	// MongoDB Pojo codec does not support arrays
	private List<Double> _coordinates;
	
	public SubObject() {}

	public SubObject(String type, List<Double> coordinates) {
		_type = type;
		_coordinates = coordinates;
	}

	@JsonProperty("type")
	public String getType() { return _type; }
	public void setType(String value) { _type = value; }
	
	@JsonProperty("coordinates")
	public List<Double> getCoordinates() { return _coordinates; }
	public void setCoordinates(List<Double> value) { _coordinates = value; }
}
