package org.pipservices.mongodb.codecs;

import java.time.Duration;

import org.bson.*;
import org.bson.codecs.*;

public class DurationStringCodec implements Codec<Duration> {

	@Override
	public Class<Duration> getEncoderClass() {
		return Duration.class;
	}

	@Override
	public void encode(BsonWriter writer, Duration value, EncoderContext encoderContext) {
		writer.writeString(value.toString());
	}

	@Override
	public Duration decode(BsonReader reader, DecoderContext decoderContext) {
		return Duration.parse(reader.readString());
	}

}