package org.apache.dkv.storage.compact;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collections;
import org.apache.dkv.storage.disk.DiskStorage;
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
        
        TestUtil.createSSTables(diskStorage);
        assertThat(diskStorage.getMaxTableId(), equalTo(1));
        assertThat(diskStorage.getTables().size(), equalTo(2));
        
        // do compact operation
        Compactor compactor = new DefaultCompactor(diskStorage);
        compactor.compact();
        
        assertThat(diskStorage.getMaxTableId(), equalTo(2));
        assertThat(diskStorage.getTables().size(), equalTo(1));
    }
}
