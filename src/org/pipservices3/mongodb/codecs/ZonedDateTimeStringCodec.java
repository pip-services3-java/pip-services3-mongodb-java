package org.pipservices3.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.pipservices3.commons.convert.DateTimeConverter;
import org.pipservices3.commons.convert.StringConverter;

import java.time.ZonedDateTime;

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