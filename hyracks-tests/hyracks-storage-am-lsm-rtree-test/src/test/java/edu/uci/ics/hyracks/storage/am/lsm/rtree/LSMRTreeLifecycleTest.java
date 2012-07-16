package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.ITreeIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

public class LSMRTreeLifecycleTest extends AbstractIndexLifecycleTest {

    private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private final IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils
            .createPrimitiveValueProviderFactories(4, IntegerPointable.FACTORY);
    private final int numKeys = 4;

    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();

    private ITreeIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    protected boolean persistentStateExists() throws Exception {
        // make sure all of the directories exist
        for (IODeviceHandle handle : harness.getIOManager().getIODevices()) {
            if (!new FileReference(handle, harness.getFileReference().getFile().getPath()).getFile().exists()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        return ((LSMRTree) index).isEmptyIndex();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = LSMRTreeTestContext.create(harness.getMemBufferCache(), harness.getMemFreePageManager(),
                harness.getIOManager(), harness.getFileReference(), harness.getDiskBufferCache(),
                harness.getDiskFileMapProvider(), fieldSerdes, valueProviderFactories, numKeys, RTreePolicyType.RTREE,
                harness.getFlushController(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler());
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws Exception {
        index.deactivate();
        index.destroy();
        harness.tearDown();
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

}