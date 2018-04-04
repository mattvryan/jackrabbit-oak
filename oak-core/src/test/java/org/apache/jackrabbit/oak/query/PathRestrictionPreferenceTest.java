package org.apache.jackrabbit.oak.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexPlan;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.text.ParseException;
import java.util.List;

import static junit.framework.TestCase.fail;

public class PathRestrictionPreferenceTest {
    static Logger logger = LoggerFactory.getLogger(PathRestrictionPreferenceTest.class);

    private boolean commitHappened;

    private static int nChildren = 5;
    private static int nLevels = 5;
    private static int nodeCount = (int)(Math.pow(5,5)+Math.pow(5,4)+Math.pow(5,3)+Math.pow(5,2)+5);

    private static final String NODE_CONTENT = "content";
    private static final String PROP_COUNTER = "counter";
    private static final String PROP_CHARGE = "charge";

    @Test
    public void testSimple() throws LoginException, NoSuchWorkspaceException,
            CommitFailedException, ParseException {
        NodeStore nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new TestIndexProvider())
                ;
        ContentRepository repo = oak.createContentRepository();
        ContentSession session = repo.login(null, null);

        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder contentNode = rootBuilder.child(NODE_CONTENT);
        createContentTree(contentNode, nChildren, nLevels, true);
        mergeAndWait(nodeStore, rootBuilder);

        QueryString counterQuery = query().where(PROP_COUNTER, 0).path(NODE_CONTENT).explain();
        QueryString chargeQuery = query().where(PROP_CHARGE, false).path(NODE_CONTENT);

        Root root = session.getLatestRoot();
        QueryEngine queryEngine = root.getQueryEngine();
        Result result = queryEngine.executeQuery(counterQuery.toString(),
                QueryEngineImpl.SQL2,
                QueryEngine.NO_BINDINGS,
                QueryEngine.NO_MAPPINGS);
    }

    private void createContentTree(NodeBuilder builder, int nChildren, int nLevels, boolean charge) {
        if (0 == nLevels || 0 == nChildren) return;
        for (int i=0; i<nChildren; i++) {
            NodeBuilder child = builder.child(String.format("Node_%d", i));
            child.setProperty(PROP_COUNTER, i);
            child.setProperty(PROP_CHARGE, charge);
            charge = ! charge;
            createContentTree(child, nChildren, nLevels-1, charge);
        }
    }

    private void traverseBranch(ChildNodeEntry node, String indent) {
        PropertyState counterProp = node.getNodeState().getProperty(PROP_COUNTER);
        String counter = counterProp != null ? Long.toString(counterProp.getValue(Type.LONG)) : "<none>";
        PropertyState chargeProp = node.getNodeState().getProperty(PROP_CHARGE);
        String charge = chargeProp != null ?
                (chargeProp.getValue(Type.BOOLEAN) ? "+" : "-") : "<none>";
        logger.info(String.format("%s%s - Counter: %s, Charge: %s", indent, node.getName(), counter, charge));
        for (ChildNodeEntry child : node.getNodeState().getChildNodeEntries()) {
            traverseBranch(child, indent+"    ");
        }
    }

    private void mergeAndWait(NodeStore nodeStore, NodeBuilder nodeBuilder) throws CommitFailedException {
        commitHappened = false;
        nodeStore.merge(nodeBuilder, new CommitHook() {
            @Nonnull
            @Override
            public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
                commitHappened = true;
                return after;
            }
        }, CommitInfo.EMPTY);
        try {
            int nTries = 0;
            while (! commitHappened && nTries < 10) {
                nTries++;
                Thread.sleep(100);
            }
        }
        catch (InterruptedException e) { }
        if (! commitHappened) {
            fail("Node store merge timeout");
        }
    }

    private QueryString query() {
        return new QueryString("select * from [nt:base]");
    }

    private static class QueryString {
        private String query;

        QueryString(String select) {
            query = String.format("%s as a", select);
        }

        @Override
        public String toString() {
            return query;
        }

        QueryString where(String propertyName, int value) {
            query = String.format("%s where [%s] = %d", query, propertyName, value);
            return this;
        }

        QueryString where(String propertyName, boolean value) {
            query = String.format("%s where [%s] = %b", query, propertyName, value);
            return this;
        }

        QueryString path(String pathRestriction) {
            query = String.format("%s and isdescendantnode(a, '/%s')", query, pathRestriction);
            return this;
        }

        QueryString explain() {
            query = String.format("explain %s", query);
            return this;
        }
    }

    private static class TestIndexProvider implements QueryIndexProvider {
        QueryIndex counterIndex = new CounterIndex();
        QueryIndex chargeIndex = new ChargeIndex();
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.of(counterIndex, chargeIndex);
        }
    }

    private static abstract class AbstractTestIndex
            implements QueryIndex, QueryIndex.AdvancedQueryIndex {
        IndexPlan indexPlan;
        String indexName;

        protected AbstractTestIndex(IndexPlan indexPlan, String indexName) {
            this.indexPlan = indexPlan;
            this.indexName = indexName;
        }

        @Override
        public double getMinimumCost() { return PropertyIndexPlan.COST_OVERHEAD + 0.1; }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return indexPlan.getPlanName();
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            indexPlan.setFilter(filter);
            return Lists.newArrayList(indexPlan);
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return null;
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) { return null; }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) { return null; }
    }

    private static class CounterIndex extends AbstractTestIndex {
        CounterIndex() {
            super(new CounterIndexPlan(), "counter-index");
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return null != filter.getPropertyRestriction(PROP_COUNTER) ? 100 : Double.POSITIVE_INFINITY;
        }
    }

    private static class ChargeIndex extends AbstractTestIndex {
        ChargeIndex() {
            super(new ChargeIndexPlan(), "charge-index");
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return null != filter.getPropertyRestriction(PROP_CHARGE) ? 500 : Double.POSITIVE_INFINITY;
        }
    }

    private static abstract class AbstractTestIndexPlan implements QueryIndex.IndexPlan {
        private Filter filter = null;
        protected String planName;

        protected AbstractTestIndexPlan(String planName) {
            this.planName = planName;
        }

        @Override
        public double getCostPerExecution() {
            if (filterMatchesPlan(filter)) {
                return 2.0;
            }
            return Double.POSITIVE_INFINITY;
        }

        protected abstract boolean filterMatchesPlan(Filter filter);

        @Override
        public double getCostPerEntry() {
            return 1.0;
        }

        @Override
        public Filter getFilter() {
            return filter;
        }

        @Override
        public void setFilter(Filter filter) {
            this.filter = filter;
        }

        @Override
        public boolean isDelayed() {
            return false;
        }

        @Override
        public boolean isFulltextIndex() {
            return false;
        }

        @Override
        public boolean includesNodeData() {
            return false;
        }

        @Override
        public List<QueryIndex.OrderEntry> getSortOrder() {
            return null;
        }

        @Override
        public NodeState getDefinition() {
            return null;
        }

        @Override
        public String getPathPrefix() {
            return null;
        }

        @CheckForNull
        @Override
        public Filter.PropertyRestriction getPropertyRestriction() {
            return null;
        }

        @Override
        public QueryIndex.IndexPlan copy() {
            return null;
        }

        @CheckForNull
        @Override
        public Object getAttribute(String name) {
            return null;
        }

        @CheckForNull
        @Override
        public String getPlanName() {
            return planName;
        }

        @Override
        public boolean getSupportsPathRestriction() {
            return (null != filter) && (null != filter.getPathRestriction()) &&
                    (0 == filter.getPathRestriction().compareTo(Filter.PathRestriction.ALL_CHILDREN) &&
                    filter.getPath().equals(String.format("/%s", NODE_CONTENT)));
        }
    }

    private static class CounterIndexPlan extends AbstractTestIndexPlan {
        CounterIndexPlan() {
            super("counter-index-plan");
        }

        @Override
        public long getEstimatedEntryCount() {
            return (long)(nodeCount / (nChildren*2));
        }

        @Override
        protected boolean filterMatchesPlan(Filter filter) {
            return null != filter && null != filter.getPropertyRestriction(PROP_COUNTER);
        }
    }

    private static class ChargeIndexPlan extends AbstractTestIndexPlan {
        ChargeIndexPlan() {
            super("charge-index-plan");
        }

        @Override
        public long getEstimatedEntryCount() {
            return (long)(nodeCount / 2);
        }

        @Override
        protected boolean filterMatchesPlan(Filter filter) {
            return null != filter && null != filter.getPropertyRestriction(PROP_CHARGE);
        }
    }
}
