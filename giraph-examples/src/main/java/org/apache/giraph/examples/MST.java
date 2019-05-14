package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.io.formats.MST.VV;
import org.apache.giraph.io.formats.MST.EV;
import org.apache.giraph.io.formats.MST.MV;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.aggregators.IntSumAggregator;

@Algorithm(name="Minimum Spanning Tree",description="Boruvka's graph shrinking algorithm")

public class MST extends BasicComputation <IntWritable,VV,EV,MV> {
	private static final String STEP="step";
	private static final String DONE="done";

	@Override
	public void compute(Vertex <IntWritable,VV,EV> vertex,Iterable <MV> messages) throws IOException {
		if (getSuperstep()==148){
			vertex.voteToHalt();
                        return;}

		int id=vertex.getId().get();
		int step=((IntWritable)getAggregatedValue(STEP)).get();

		if (id==0) {
			int src=vertex.getValue().src;
			int dst=vertex.getValue().dst;
			int fml=vertex.getValue().family;
			if (step==0) vertex.setValue(new VV(fml+1,src,dst));
			if (step==3) vertex.setValue(new VV(fml,src+1,dst));
			if (step==4) vertex.setValue(new VV(fml,src,dst+1));
			return;
			}

		if (getSuperstep()==0) {
			if (id==0) { vertex.voteToHalt(); return; }

			int cost;
			int mdest=-1;
			int mcost=Integer.MAX_VALUE;

			for (Edge <IntWritable,EV> edge : vertex.getEdges()) {
				cost=edge.getValue().cost;
				if (cost<mcost) {
					mcost=cost;
					mdest=edge.getTargetVertexId().get();
					}
				}

			vertex.setValue(new VV(-1,id,mdest));

			for (Edge <IntWritable,EV> edge : vertex.getEdges())
				if (edge.getTargetVertexId().get()==mdest) {
					sendMessage(edge.getTargetVertexId(),new MV(-1,id,-1,-1));
					vertex.setEdgeValue(edge.getTargetVertexId(),new EV(mcost,true));
					}

			//vertex.voteToHalt();
			return;
			}

		if (getSuperstep()==1) {
			boolean supervertex=false;
			int pick=vertex.getValue().dst;

			for (MV message : messages) {
				IntWritable src=new IntWritable(message.src);
				vertex.setEdgeValue(src,new EV(vertex.getEdgeValue(src).cost,true));
				if (message.src==pick && id<message.src) supervertex=true;
				}

			if (supervertex)
				for (Edge <IntWritable,EV> edge : vertex.getEdges())
					if (edge.getValue().taken)
						sendMessage(edge.getTargetVertexId(),new MV(-1,id,-1,-1));

			vertex.voteToHalt();
			return;
			}

		if (step==0) {
			for (Edge <IntWritable,EV> edge : vertex.getEdges())
				if (!edge.getValue().taken)
					sendMessage(edge.getTargetVertexId(),new MV(vertex.getValue().family,id,edge.getValue().cost,-1));
			}

		if (step==1) {
			int cost;
			int mfaml=-1;
			int mdest=-1;
			int mcost=Integer.MAX_VALUE;
			int family=vertex.getValue().family;

			for (MV message : messages)
				if (message.val!=family) {
					cost=message.dst;
					if (cost<mcost) {
						mcost=cost;
						mdest=message.src;
						mfaml=message.val;
						}
					}

			if (mdest!=-1) sendMessage(new IntWritable(Math.abs(family)),new MV(mcost,id,mdest,mfaml));

			vertex.voteToHalt();
			return;
			}

		if (step==2) {
			int mfml=-1;
			int msrc=-1;
			int mdst=-1;
			int mcost=Integer.MAX_VALUE;

			for (MV message : messages)
				if (message.val<mcost) {
					mfml=message.tag;
					msrc=message.src;
					mdst=message.dst;
					mcost=message.val;
					}

			int family=vertex.getValue().family;
			vertex.setValue(new VV(family,msrc,mdst));

			sendMessage(new IntWritable(Math.abs(mfml)),new MV(Math.abs(family),msrc,mdst,1));
			sendMessage(new IntWritable(msrc),new MV(-1,-1,mdst,-1));
			sendMessage(new IntWritable(mdst),new MV(-1,-1,msrc,-1));

			vertex.voteToHalt();
			return;
			}

		if (step==3) {
			int src=vertex.getValue().src;
			int dst=vertex.getValue().dst;
			int family=vertex.getValue().family;

			for (MV message : messages) {
				if (message.tag==-1) {
					IntWritable next=new IntWritable(message.dst);
					vertex.setEdgeValue(next,new EV(vertex.getEdgeValue(next).cost,true));
					}
				else
					if (message.val>Math.abs(family) && (message.dst==src || message.src==dst)) {
						vertex.setValue(new VV(-family,src,dst));

						for (Edge <IntWritable,EV> edge : vertex.getEdges())
							if (edge.getValue().taken)
								sendMessage(edge.getTargetVertexId(),new MV(-1,-family,-1,-1));
						}
				}

			vertex.voteToHalt();
			return;
			}

		if (step==4) {
			int family=vertex.getValue().family;
			for (MV message : messages)
				if (message.src!=family) {
					vertex.setValue(new VV(message.src,vertex.getValue().src,vertex.getValue().dst));
					for (Edge <IntWritable,EV> edge : vertex.getEdges())
						if (edge.getValue().taken)
							sendMessage(edge.getTargetVertexId(),new MV(-1,message.src,-1,-1));
					}

			aggregate(DONE,new IntWritable(1));
			}
		}

	public static class Aggregators extends DefaultMasterCompute {
	    @Override
	    public void compute() {
	    	if (getSuperstep()==0) {
	            setAggregatedValue(DONE,new IntWritable(0));
	            setAggregatedValue(STEP,new IntWritable(4));
	            return;
	    	    }

	        int V=(int)getTotalNumVertices()-1;
	        int done=((IntWritable)getAggregatedValue(DONE)).get();
	        int step=((IntWritable)getAggregatedValue(STEP)).get();

	        if (step<=3) step++;
	        else if (done==V) step=0;

	        setAggregatedValue(DONE,new IntWritable(0));
	        setAggregatedValue(STEP,new IntWritable(step));
	        }

	        @Override
        	public void initialize() throws InstantiationException,IllegalAccessException {
	            registerPersistentAggregator(DONE,IntSumAggregator.class);
        	    registerPersistentAggregator(STEP,IntSumAggregator.class);
            	}
        }
}
