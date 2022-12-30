package org.apache.nifi.processors.msgpack;

import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.MockFlowFile;

import org.junit.Before;
import org.junit.Test;

public class TestConvertMsgPackToJSON {

    private static final String CONTENT = "{\"message\":\"Hello, World!\"}";
    private static final int[] MSGPACK_INTS = {129, 167, 109, 101, 115, 115, 97, 103, 101, 173, 72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
    private static byte[] MSGPACK_BYTES;

    private TestRunner runner;

    @Before
    public void init() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes);
        for (int i : MSGPACK_INTS) {
            data.write(i);
        }
        MSGPACK_BYTES = bytes.toByteArray();

        runner = TestRunners.newTestRunner(ConvertMsgPackToJSON.class);
    }

    @Test
    public void testPackJson() throws IOException {
        final Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("mime.type", "application/msgpack");

        runner.enqueue(MSGPACK_BYTES, attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToMsgPack.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToMsgPack.REL_FAILURE, 0);

        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(ConvertMsgPackToJSON.REL_SUCCESS)) {
            flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
            flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
            flowFile.assertAttributeExists("mime.extension");
            flowFile.assertAttributeEquals("mime.extension", ".json");
            flowFile.assertContentEquals(CONTENT);
        }
    }
    
}
