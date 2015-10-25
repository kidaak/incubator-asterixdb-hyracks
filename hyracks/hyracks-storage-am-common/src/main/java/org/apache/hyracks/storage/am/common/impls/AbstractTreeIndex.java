/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.common.impls;

import java.util.ArrayList;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractTreeIndex extends AbstractFileManager implements ITreeIndex {

    protected final static int rootPage = 1;

    protected final IFreePageManager freePageManager;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    public AbstractTreeIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IFreePageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            FileReference file) {
        super(bufferCache, fileMapProvider, file);
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
    }

    void initEmptyTree() throws HyracksDataException {
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.init(metaFrame, rootPage);

        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        try {
            frame.setPage(rootNode);
            frame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch(true);
            bufferCache.unpin(rootNode);
        }
    }

    public boolean isEmptyTree(ITreeIndexFrame frame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            if (frame.getLevel() == 0 && frame.getTupleCount() == 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public byte getTreeHeight(ITreeIndexFrame frame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            return frame.getLevel();
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    @Override
    public int getRootPageId() {
        return rootPage;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();

        freePageManager.open(fileId);
        initEmptyTree();
        freePageManager.close();
        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();

        initEmptyTree();
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        super.deactivate();
        freePageManager.close();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        freePageManager.open(fileId);

        // TODO: Should probably have some way to check that the tree is physically consistent
        // or that the file we just opened actually is a tree
    }

    public abstract class AbstractTreeIndexBulkLoader implements IIndexBulkLoader {
        protected final MultiComparator cmp;
        protected final int slotSize;
        protected final int leafMaxBytes;
        protected final int interiorMaxBytes;
        protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        protected final ITreeIndexMetaDataFrame metaFrame;
        protected final ITreeIndexTupleWriter tupleWriter;
        protected ITreeIndexFrame leafFrame;
        protected ITreeIndexFrame interiorFrame;
        private boolean releasedLatches;

        public AbstractTreeIndexBulkLoader(float fillFactor) throws TreeIndexException, HyracksDataException {
            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();
            metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();

            if (!isEmptyTree(leafFrame)) {
                throw new TreeIndexException("Cannot bulk-load a non-empty tree.");
            }

            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId), true);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) (interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) (leafFrame.getBuffer().capacity() * fillFactor);
            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
        }

        @Override
        public abstract void add(ITupleReference tuple) throws IndexException, HyracksDataException;

        protected void handleException() throws HyracksDataException {
            // Unlatch and unpin pages.
            for (NodeFrontier nodeFrontier : nodeFrontiers) {
                nodeFrontier.page.releaseWriteLatch(true);
                bufferCache.unpin(nodeFrontier.page);
            }
            releasedLatches = true;
        }

        @Override
        public void end() throws HyracksDataException {
            // copy the root generated from the bulk-load to *the* root page location
            ICachedPage newRoot = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
            newRoot.acquireWriteLatch();
            NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
            try {
                System.arraycopy(lastNodeFrontier.page.getBuffer().array(), 0, newRoot.getBuffer().array(), 0,
                        lastNodeFrontier.page.getBuffer().capacity());
            } finally {
                newRoot.releaseWriteLatch(true);
                bufferCache.unpin(newRoot);

                // register old root as a free page
                freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

                if (!releasedLatches) {
                    for (int i = 0; i < nodeFrontiers.size(); i++) {
                        try {
                            nodeFrontiers.get(i).page.releaseWriteLatch(true);
                        } catch (Exception e) {
                            //ignore illegal monitor state exception
                        }
                        bufferCache.unpin(nodeFrontiers.get(i).page);
                    }
                }
            }
        }

        protected void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = freePageManager.getFreePage(metaFrame);
            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }

        public ITreeIndexFrame getLeafFrame() {
            return leafFrame;
        }

        public void setLeafFrame(ITreeIndexFrame leafFrame) {
            this.leafFrame = leafFrame;
        }
    }

    public class TreeIndexInsertBulkLoader implements IIndexBulkLoader {
        ITreeIndexAccessor accessor;

        public TreeIndexInsertBulkLoader() throws HyracksDataException {
            accessor = (ITreeIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            try {
                accessor.insert(tuple);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void end() throws HyracksDataException {
            // do nothing
        }

    }

    @Override
    public long getMemoryAllocationSize() {
        return 0;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    @Override
    public boolean hasMemoryComponents() {
        return true;
    }
}
