package org.pipservices.fixtures;

import java.time.ZonedDateTime;
import java.util.List;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.pipservices.commons.data.IStringIdentifiable;

public class Dummy implements IStringIdentifiable{	

	public Dummy(String id, String key, String content, ZonedDateTime createTimeUtc, InnerDummy innerDummy,
			List<InnerDummy> innerDummies) {
		super();
		this.id = id;
		this.key = key;
		this.content = content;
		this.createTimeUtc = createTimeUtc;
		this.innerDummy = innerDummy;
		this.innerDummies = innerDummies;
	}

	@BsonProperty("id")
	private String id;
	public String getId() {	return id; }
	public void setId(String id) {	this.id = id;}
	
	@BsonProperty("key")
	private String key;
	public String getKey() { return key; }
	public void setKey(String key) { this.key = key; }

	@BsonProperty("content")
	private String content;
	public String getContent() { return content; }
	public void setContent(String content) { this.content = content; }

	@BsonProperty("create_time_utc")
	private ZonedDateTime createTimeUtc;
	public ZonedDateTime getCreateTimeUtc() { return createTimeUtc; }
	public void setCreateTimeUtc(ZonedDateTime createTimeUtc) { this.createTimeUtc = createTimeUtc; }

	@BsonProperty("inner_dummy")
	private InnerDummy innerDummy;
	public InnerDummy getInnerDummy() { return innerDummy; }
	public void setInnerDummy(InnerDummy innerDummy) { this.innerDummy = innerDummy; }

	@BsonProperty("inner_dummies")
	private List<InnerDummy> innerDummies;
	public List<InnerDummy> getInnerDummies() { return innerDummies; }
	public void setInnerDummies(List<InnerDummy> innerDummies) { this.innerDummies = innerDummies; }
}
