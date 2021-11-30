package org.apache.dkv.storage.wal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class WALWriterTest {

    private final TemporaryFolder folder = new TemporaryFolder();
    
    @Before
    public void setUp() {
        try {
            folder.create();
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    @Test
    public void testRecordType() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.flush);
        
        WALWriter wal = new WALWriter(config);
        Method method = wal.getClass().getDeclaredMethod("getRecordType", boolean.class, boolean.class);
        method.setAccessible(true);
        
        assertThat(method.invoke(wal, true, true), equalTo(RecordType.FullType));
        assertThat(method.invoke(wal, true, false), equalTo(RecordType.FirstType));
        assertThat(method.invoke(wal, false, false), equalTo(RecordType.MiddleType));
        assertThat(method.invoke(wal, false, true), equalTo(RecordType.LastType));
    }
    
    @Test
    public void testGetMaxTableId() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.flush);

        WALWriter wal = new WALWriter(config);
        Method method = wal.getClass().getDeclaredMethod("getMaxTableId");
        method.setAccessible(true);
        assertThat(method.invoke(wal), equalTo(0));
        folder.newFile("dkv11.wal");
        assertThat(method.invoke(wal), equalTo(11));
    }
    
    @Test
    public void testAddRecord() throws IOException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.flush);

        WALWriter wal = new WALWriter(config);
        KeyValuePair a = KeyValuePair.createPut(Bytes.toBytes("A"), Bytes.toBytes("A"), 1);
        wal.addRecord(a.toBytes());
        KeyValuePair b = KeyValuePair.createPut(Bytes.toBytes("B"), Bytes.toBytes("B"), 1);
        wal.addRecord(b.toBytes());
        KeyValuePair c = KeyValuePair.createPut(Bytes.toBytes("C"), Bytes.toBytes("C"), 1);
        wal.addRecord(c.toBytes());
        int size = a.getSerializeSize() + b.getSerializeSize() + c.getSerializeSize();
        File f = new File(folder.getRoot().getAbsoluteFile() + File.separator + "dkv00.wal");
        assertThat(f.length(), equalTo(size + WALWriter.getHEADER_SIZE() * 3L));
    }
    
    @Test
    public void testAddRecordWithSwitchBlock() throws IOException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.sync);
        WALWriter wal = new WALWriter(config);
        KeyValuePair a = KeyValuePair.createPut(Bytes.toBytes("A"), Bytes.toBytes(repeat("a", WALWriter.getMAX_BLOCK_SIZE() / 2)), 1);
        KeyValuePair b = KeyValuePair.createPut(Bytes.toBytes("B"), Bytes.toBytes(repeat("b", WALWriter.getMAX_BLOCK_SIZE() / 2)), 1);
        KeyValuePair c = KeyValuePair.createPut(Bytes.toBytes("C"), Bytes.toBytes(repeat("c", WALWriter.getMAX_BLOCK_SIZE() / 2)), 1);
        wal.addRecord(a.toBytes());
        wal.addRecord(b.toBytes());
        wal.addRecord(c.toBytes());
        File f = new File(folder.getRoot().getAbsoluteFile() + File.separator + "dkv00.wal");
        // because b Split into two blocks, There is 4 block header.
        int size = a.getSerializeSize() + b.getSerializeSize() + c.getSerializeSize();
        assertThat(f.length(), equalTo(size + WALWriter.getHEADER_SIZE() * 4L));
    }
    
    @Test
    public void testAddRecordContainAllType() throws IOException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.sync);
        WALWriter wal = new WALWriter(config);
        KeyValuePair a = KeyValuePair.createPut(Bytes.toBytes("A"), Bytes.toBytes(repeat("a", WALWriter.getMAX_BLOCK_SIZE() * 3)), 1);
        int size = a.getSerializeSize();
        wal.addRecord(a.toBytes());
        File f = new File(folder.getRoot().getAbsoluteFile() + File.separator + "dkv00.wal");
        // Need 4 blocks to store all the data
        assertThat(f.length(), equalTo(size + WALWriter.getHEADER_SIZE() * 4L));
    }
    
    @Test
    public void testAddRecordWithSwitchFile() throws IOException {
        Config config = mock(Config.class);
        when(config.getDataDir()).thenReturn(folder.getRoot().getAbsolutePath());
        when(config.getWriteOptions()).thenReturn(WriteOptions.sync);
        WALWriter wal = new WALWriter(config);
        KeyValuePair a = KeyValuePair.createPut(Bytes.toBytes("A"), Bytes.toBytes(repeat("a", WALWriter.getMAX_BLOCK_SIZE() * 3)), 1);
        wal.addRecord(a.toBytes());
        wal.switchNewFile();
        KeyValuePair b = KeyValuePair.createPut(Bytes.toBytes("B"), Bytes.toBytes(repeat("b", WALWriter.getMAX_BLOCK_SIZE() * 4)), 1);
        int size = b.getSerializeSize();
        wal.addRecord(b.toBytes());
        File f = new File(folder.getRoot().getAbsoluteFile() + File.separator + "dkv01.wal");
        // Need 5 blocks to store all the data
        assertThat(f.length(), equalTo(size + WALWriter.getHEADER_SIZE() * 5L));
    }
    
    private String repeat(final String source, final int times) {
        StringBuilder buffer = new StringBuilder(source.length() * times);
        for (int i = 0; i < times; i++) {
            buffer.append(source);
        }
        return buffer.toString();
    }
    
    @After
    public void tearDown() {
        folder.delete();
    }
}
