package org.apache.dkv.storage.wal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

public class RecordTypeTest {

    @Test
    public void testRecordType() {
        assertThat(RecordType.convertCodeToRecordType((byte) 0), equalTo(RecordType.ZeroType));
        assertThat(RecordType.convertCodeToRecordType((byte) 1), equalTo(RecordType.FullType));
        assertThat(RecordType.convertCodeToRecordType((byte) 2), equalTo(RecordType.FirstType));
        assertThat(RecordType.convertCodeToRecordType((byte) 3), equalTo(RecordType.MiddleType));
        assertThat(RecordType.convertCodeToRecordType((byte) 4), equalTo(RecordType.LastType));
    }
}
