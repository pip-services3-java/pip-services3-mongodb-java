package org.pipservices.mongodb.persistence;

import java.util.*;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.pipservices.commons.data.IStringIdentifiable;

public class Dummy implements IStringIdentifiable{	

	public Dummy() {}
	
	public Dummy(String id, String key, String content) {
		super();
		this._id = id;
		this._key = key;
		this._content = content;
		this._createTime = new Date();
	}
	
	@BsonProperty("_id")
	private String _id;
	public String getId() {	return _id; }
	public void setId(String id) {	this._id = id;}
	
	@BsonProperty("key")
	private String _key;
	public String getKey() { return _key; }
	public void setKey(String key) { this._key = key; }

	@BsonProperty("content")
	private String _content;
	public String getContent() { return _content; }
	public void setContent(String content) { this._content = content; }

	@BsonProperty("create_time")
	private Date _createTime;
	public Date getCreateTime() { return _createTime; }
	public void setCreateTime(Date createTime) { this._createTime = createTime; }
}
