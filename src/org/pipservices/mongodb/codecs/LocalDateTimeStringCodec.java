package org.pipservices.mongodb.codecs;

import java.time.LocalDateTime;

import org.bson.*;
import org.bson.codecs.*;

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