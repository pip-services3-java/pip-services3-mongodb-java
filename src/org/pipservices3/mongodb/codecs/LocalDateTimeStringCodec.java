package org.pipservices3.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.time.LocalDateTime;

public class LocalDateTimeStringCodec implements Codec<LocalDateTime> {

	@Override
	public Class<LocalDateTime> getEncoderClass() {
		return LocalDateTime.class;
	}

	@Override
	public void encode(BsonWriter writer, LocalDateTime value,
		EncoderContext encoderContext) {
		writer.writeString(value.toString());
	}

	@Override
	public LocalDateTime decode(BsonReader reader, DecoderContext decoderContext) {
		return LocalDateTime.parse(reader.readString());
	}

}