package org.pipservices3.mongodb.codecs;

import java.time.Duration;

import org.bson.*;
import org.bson.codecs.*;

public class DurationInt64Codec implements Codec<Duration> {

	@Override
	public Class<Duration> getEncoderClass() {
		return Duration.class;
	}

	@Override
	public void encode(BsonWriter writer, Duration value, EncoderContext encoderContext) {
		writer.writeInt64(value.getSeconds());
	}

	@Override
	public Duration decode(BsonReader reader, DecoderContext decoderContext) {
		return Duration.ofSeconds(reader.readInt64());
	}

}