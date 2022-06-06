package org.pipservices3.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.Duration;

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