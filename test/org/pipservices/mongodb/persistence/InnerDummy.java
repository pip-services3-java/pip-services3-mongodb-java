package org.pipservices.mongodb.persistence;

import java.util.List;

import org.bson.codecs.pojo.annotations.BsonProperty;

public class InnerDummy {

	public InnerDummy() {}
	
	public InnerDummy(String id, String name, String description) {
		super();
		this.id = id;
		this.name = name;
		this.description = description;
	}

	@BsonProperty("id")
	private String id;
	public String getId() { return id; }
	public void setId(String id) { this.id = id; }
	
	@BsonProperty("name")
	private String name;
	public String getName() { return name; }
	public void setName(String name) { this.name = name; }

	@BsonProperty("description")
	private String description;
	public String getDescription() { return description; }
	public void setDescription(String description) { this.description = description; }

	@BsonProperty("inner_inner_dummies")
	private List<InnerDummy> innerInnerDummies;
	public List<InnerDummy> getInnerInnerDummies() { return innerInnerDummies; }
	public void setInnerInnerDummies(List<InnerDummy> innerInnerDummies) { this.innerInnerDummies = innerInnerDummies; }
}
