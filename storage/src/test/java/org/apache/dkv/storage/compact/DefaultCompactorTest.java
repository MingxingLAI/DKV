package org.apache.dkv.storage.compact;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.flush.DefaultFlusher;
import org.apache.dkv.storage.flush.Flusher;
import org.apache.dkv.storage.iterator.MemStoreIterator;
import org.apache.dkv.storage.iterator.SeekIterator;
import org.apache.dkv.storage.memory.MemStore;
import org.apache.dkv.storage.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DefaultCompactorTest {

    private final TemporaryFolder folder = new TemporaryFolder();

    private DiskStorage diskStorage;
    
    @Before
    public void setUp() {
        try {
            folder.create();
            diskStorage = new DiskStorage(folder.getRoot().getAbsolutePath(), 10);
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    @Test
    public void testFlush() throws IOException {
        diskStorage.open();
        assertThat(diskStorage.getMaxTableId(), equalTo(-1));
        assertThat(diskStorage.getTables(), equalTo(Collections.emptyList()));
        
        createSSTables();
        assertThat(diskStorage.getMaxTableId(), equalTo(1));
        assertThat(diskStorage.getTables().size(), equalTo(2));
        
        // do compact operation
        Compactor compactor = new DefaultCompactor(diskStorage);
        compactor.compact();
        
        assertThat(diskStorage.getMaxTableId(), equalTo(2));
        assertThat(diskStorage.getTables().size(), equalTo(1));

        int count = 0;
        SeekIterator<KeyValuePair> iterator = diskStorage.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, equalTo(12));
    }
    
    private void createSSTables() throws IOException {
        Flusher flusher = new DefaultFlusher(diskStorage);
        MemStoreIterator memStoreIterator = createNewMemStore(Arrays.asList("1", "3", "5"), Arrays.asList("2", "4", "6"));
        flusher.flush(memStoreIterator);

        memStoreIterator = createNewMemStore(Arrays.asList("a", "c", "e"), Arrays.asList("b", "d", "f"));
        flusher.flush(memStoreIterator);
    }
    
    private MemStoreIterator createNewMemStore(final List<String> data, final List<String> snapshot) throws IOException {
        final MemStore memStore = mock(MemStore.class);
        when(memStore.getKvMap()).thenReturn(TestUtil.createKeyValuePairMap(data));
        when(memStore.getSnapshot()).thenReturn(TestUtil.createKeyValuePairMap(snapshot));
        return new MemStoreIterator(memStore);
    }
}
