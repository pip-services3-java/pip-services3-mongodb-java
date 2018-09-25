package org.pipservices.mongodb.codecs;

import java.time.LocalDate;

import org.bson.*;
import org.bson.codecs.*;

public class LocalDateStringCodec implements Codec<LocalDate> {

	@Override
	public Class<LocalDate> getEncoderClass() {
		return LocalDate.class;
	}

	@Override
	public void encode(BsonWriter writer, LocalDate value,
		EncoderContext encoderContext) {
		writer.writeString(value.toString());
	}

	@Override
	public LocalDate decode(BsonReader reader, DecoderContext decoderContext) {
		return LocalDate.parse(reader.readString());
	}

}