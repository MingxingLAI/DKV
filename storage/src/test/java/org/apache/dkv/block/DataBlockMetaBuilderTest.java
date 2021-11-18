package org.apache.dkv.block;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.block.DataBlockMeta;
import org.apache.dkv.storage.block.DataBlockMetaBuilder;
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Test;

public class DataBlockMetaBuilderTest {

    @Test
    public void testDataBlockMetaBuilder() throws IOException {
        DataBlockMetaBuilder builder = new DataBlockMetaBuilder();
        builder.append(KeyValuePair.createPut(Bytes.toBytes(1), Bytes.toBytes(1), 1), 100, 100, Bytes.EMPTY_BYTES);
        builder.append(KeyValuePair.createPut(Bytes.toBytes(2), Bytes.toBytes(2), 2), 200, 101, Bytes.EMPTY_BYTES);
    
        byte[] result = builder.serialize();
        DataBlockMeta meta1 = DataBlockMeta.parseFrom(result, 0);
        assertThat(meta1.getBlockOffset(), equalTo(100L));
        assertThat(meta1.getBlockSize(), equalTo(100L));
        assertThat(meta1.getLastKv(), equalTo(KeyValuePair.createPut(Bytes.toBytes(1), Bytes.toBytes(1), 1)));
        DataBlockMeta meta2 = DataBlockMeta.parseFrom(result, result.length / 2);
        assertThat(meta2.getBlockOffset(), equalTo(200L));
        assertThat(meta2.getBlockSize(), equalTo(101L));
        assertThat(meta2.getLastKv(), equalTo(KeyValuePair.createPut(Bytes.toBytes(2), Bytes.toBytes(2), 2)));
    }
}
