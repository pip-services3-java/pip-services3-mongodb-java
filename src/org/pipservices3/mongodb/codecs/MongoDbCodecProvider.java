package org.pipservices3.mongodb.codecs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.time.ZonedDateTime;

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