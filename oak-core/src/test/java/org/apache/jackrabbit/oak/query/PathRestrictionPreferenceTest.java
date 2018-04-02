package org.apache.jackrabbit.oak.query;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
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
import java.util.List;

import static junit.framework.TestCase.fail;

public class PathRestrictionPreferenceTest {
    static Logger logger = LoggerFactory.getLogger(PathRestrictionPreferenceTest.class);

    private boolean commitHappened;

    private static int nChildren = 5;
    private static int nLevels = 5;
    private static int nodeCount = 5^5+5^4+5^3+5^2+5^1;

    @Test
    public void testSimple() throws LoginException, NoSuchWorkspaceException,
            CommitFailedException {
        NodeStore nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new TestIndexProvider())
                ;
        ContentRepository repo = oak.createContentRepository();
        ContentSession session = repo.login(null, null);
        Root root = session.getLatestRoot();
        QueryEngine queryEngine = root.getQueryEngine();

        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder contentNode = rootBuilder.child("content");
        createContentTree(contentNode, nChildren, nLevels, true);
        mergeAndWait(nodeStore, rootBuilder);

        for (ChildNodeEntry entry : nodeStore.getRoot().getChildNodeEntries()) {
            if (entry.getName() == "content") {
                traverseBranch(entry, "");
            }
        }

        String numberQuery = "select * from [nt:base] where [number] = 0";
        String polarityQuery = "select * from [nt:base] where [polarity] = cast('false' as BOOLEAN)";

//        for (String query : Lists.newArrayList(numberQuery, polarityQuery)) {
//            try {
//                Result result = queryEngine.executeQuery(query,
//                        QueryEngineImpl.SQL2,
//                        QueryEngine.NO_BINDINGS,
//                        QueryEngine.NO_MAPPINGS);
//                logger.info(result.toString());
//            } catch (ParseException e) {
//                fail(e.getMessage());
//            }
//        }
    }

    private void createContentTree(NodeBuilder builder, int nChildren, int nLevels, boolean b) {
        if (0 == nLevels || 0 == nChildren) return;
        for (int i=0; i<nChildren; i++) {
            NodeBuilder child = builder.child(String.format("Node_%d", i));
            child.setProperty("a", i);
            child.setProperty("b", b);
            b = ! b;
            createContentTree(child, nChildren, nLevels-1, b);
        }
    }

    private void traverseBranch(ChildNodeEntry node, String indent) {
        PropertyState nodeAProp = node.getNodeState().getProperty("a");
        String nodeNumber = nodeAProp != null ? Long.toString(nodeAProp.getValue(Type.LONG)) : "<none>";
        PropertyState nodeBProp = node.getNodeState().getProperty("b");
        String nodePolarity = nodeBProp != null ?
                (nodeBProp.getValue(Type.BOOLEAN) ? "+" : "-")
                : "<none>";
        logger.info(String.format("%s%s - N: %s, P: %s", indent, node.getName(), nodeNumber, nodePolarity));
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
                Thread.sleep(100);
            }
        }
        catch (InterruptedException e) { }
        if (! commitHappened) {
            fail("Node store merge timeout");
        }
    }

    private static class TestIndexProvider implements QueryIndexProvider {
        QueryIndex aIndex = new AIndex();
        QueryIndex bIndex = new BIndex();
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.of(aIndex, bIndex);
        }
    }

    private static abstract class AbstractTestIndex
            implements QueryIndex, QueryIndex.AdvancedQueryIndex {
        @Override
        public double getMinimumCost() { return PropertyIndexPlan.COST_OVERHEAD + 0.1; }

        @Override
        public Cursor query(Filter filter, NodeState rootState) { return null; }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) { return null; }
    }

    private static class AIndex extends AbstractTestIndex {
        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return null != filter.getPropertyRestriction("a") ? 100 : Double.POSITIVE_INFINITY;
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) { return null; }

        @Override
        public String getIndexName() { return "a-index"; }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            return null;
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return null;
        }
    }

    private static class BIndex extends AbstractTestIndex {
        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return null != filter.getPropertyRestriction("b") ? 500 : Double.POSITIVE_INFINITY;
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) { return null; }

        @Override
        public String getIndexName() { return "b-index"; }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            return null;
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return null;
        }
    }

    private static abstract class AbstractTestIndexPlan implements QueryIndex.IndexPlan {
        private Filter filter = null;

        @Override
        public double getCostPerExecution() {
            return 2;
        }

        @Override
        public double getCostPerEntry() {
            return 1;
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
    }

    private static class AIndexPlan extends AbstractTestIndexPlan {
        @Override
        public long getEstimatedEntryCount() {
            return (long)(nodeCount / (nChildren*2));
        }

        @Override
        public boolean getSupportsPathRestriction() {
            return false;
        }

        @CheckForNull
        @Override
        public String getPlanName() {
            return "a-index-plan";
        }
    }

    private static class BIndexPlan extends AbstractTestIndexPlan {
        @Override
        public long getEstimatedEntryCount() {
            return (long)(nodeCount / 2);
        }

        @Override
        public boolean getSupportsPathRestriction() {
            return false;
        }

        @CheckForNull
        @Override
        public String getPlanName() {
            return "b-index-plan";
        }
    }
}
