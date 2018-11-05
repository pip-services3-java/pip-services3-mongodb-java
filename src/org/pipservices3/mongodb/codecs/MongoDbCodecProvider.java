package org.pipservices3.mongodb.codecs;

import java.time.ZonedDateTime;

import org.bson.codecs.*;
import org.bson.codecs.configuration.*;

public class MongoDbCodecProvider implements CodecProvider {
    private final ZonedDateTimeStringCodec zonedDateTimeCodec = new ZonedDateTimeStringCodec();

    @SuppressWarnings("unchecked")
	@Override
    public <T> Codec<T> get(final Class<T> type, final CodecRegistry registry) {
    	if (ZonedDateTime.class.equals(type))
    		return (Codec<T>)zonedDateTimeCodec;
    	return null;
    }
}