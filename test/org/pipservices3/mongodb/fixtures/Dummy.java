package org.pipservices3.mongodb.fixtures;

import java.time.*;
import java.util.*;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.pipservices3.commons.data.IStringIdentifiable;
import org.pipservices3.commons.data.IdGenerator;
import org.pipservices3.mongodb.persistence.SubObject;

public class Dummy implements IStringIdentifiable {

    public Dummy() {
    }

    public Dummy(String id, String key, String content) {
        super();
        this._id = id;
        this._key = key;
        this._content = content;
        this._createTime = ZonedDateTime.now();
        this._subObject = new SubObject("Type", Arrays.asList(Double.valueOf(0), Double.valueOf(0)));
    }

    @BsonProperty("id")
    private String _id;

    public String getId() {
        return _id;
    }

    public void setId(String id) {
        this._id = id;
    }

    @Override
    public String withGeneratedId() {
        _id = IdGenerator.nextLong();
        return _id;
    }

    @BsonProperty("key")
    private String _key;

    public String getKey() {
        return _key;
    }

    public void setKey(String key) {
        this._key = key;
    }

    @BsonProperty("content")
    private String _content;

    public String getContent() {
        return _content;
    }

    public void setContent(String content) {
        this._content = content;
    }

    @BsonProperty("create_time")
    private ZonedDateTime _createTime;

    public ZonedDateTime getCreateTime() {
        return _createTime;
    }

    public void setCreateTime(ZonedDateTime createTime) {
        this._createTime = createTime;
    }

    @BsonProperty("sub_object")
    private SubObject _subObject;

    public SubObject getSubObject() {
        return _subObject;
    }

    public void setSubObject(SubObject value) {
        _subObject = value;
    }

}
