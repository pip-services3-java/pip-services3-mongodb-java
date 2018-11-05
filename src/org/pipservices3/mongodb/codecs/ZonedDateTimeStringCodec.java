package org.pipservices3.mongodb.codecs;

import java.time.ZonedDateTime;

import org.bson.*;
import org.bson.codecs.*;
import org.pipservices3.commons.convert.*;

public class ZonedDateTimeStringCodec implements Codec<ZonedDateTime> {

	@Override
	public Class<ZonedDateTime> getEncoderClass() {
		return ZonedDateTime.class;
	}

	@Override
	public void encode(BsonWriter writer, ZonedDateTime value, EncoderContext encoderContext) {
		//writer.writeString(value.toString());
		writer.writeString(StringConverter.toNullableString(value));
	}

	@Override
	public ZonedDateTime decode(BsonReader reader, DecoderContext decoderContext) {
		//return ZonedDateTime.parse(reader.readString());
		return DateTimeConverter.toNullableDateTime(reader.readString());
	}

}