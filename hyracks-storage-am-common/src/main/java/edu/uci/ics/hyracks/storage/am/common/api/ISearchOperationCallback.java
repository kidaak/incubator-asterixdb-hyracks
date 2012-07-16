package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ISearchOperationCallback {
    public boolean proceed(ITupleReference tuple);

    public void reconcile(ITupleReference tuple);
}