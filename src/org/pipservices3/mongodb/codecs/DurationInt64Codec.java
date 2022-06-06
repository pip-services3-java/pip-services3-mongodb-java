package org.pipservices3.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.Duration;

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