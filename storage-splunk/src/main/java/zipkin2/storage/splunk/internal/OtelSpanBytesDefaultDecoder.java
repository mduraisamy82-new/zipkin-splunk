package zipkin2.storage.splunk.internal;

import zipkin2.Span;
import zipkin2.codec.BytesDecoder;
import zipkin2.codec.Encoding;
import zipkin2.internal.JsonCodec;
import zipkin2.internal.ReadBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OtelSpanBytesDefaultDecoder implements BytesDecoder<Span> {

    @Override
    public Encoding encoding() {
        return Encoding.JSON;
    }

    @Override
    public boolean decode(byte[] bytes, Collection<Span> collection) {
        return JsonCodec.read(new OtelSpanDefaultReader(), ReadBuffer.wrap(bytes), collection);
    }

    @Override
    public Span decodeOne(byte[] bytes) {
        return (Span)JsonCodec.readOne(new OtelSpanDefaultReader(), ReadBuffer.wrap(bytes));
    }

    @Override
    public boolean decodeList(byte[] bytes, Collection<Span> collection) {
        return JsonCodec.readList(new OtelSpanDefaultReader(), ReadBuffer.wrap(bytes), collection);
    }

    @Override
    public List<Span> decodeList(byte[] bytes) {
        List<Span> spanList = new ArrayList<>();
        decodeList(bytes,spanList);
        return spanList;
    }
}
