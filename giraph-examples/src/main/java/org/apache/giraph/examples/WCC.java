package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.graph.BasicComputation;

@Algorithm(name="Weekly Connected Components",description="Find connected components of an undirected graph")

public class WCC extends BasicComputation <IntWritable,IntWritable,NullWritable,IntWritable> {
	@Override
	public void compute(Vertex <IntWritable,IntWritable,NullWritable> vertex,Iterable <IntWritable> messages) throws IOException {
		//initialize and send starting labels
		if (getSuperstep()==0) {
			vertex.setValue(new IntWritable(vertex.getId().get()));
			for (Edge <IntWritable,NullWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(),new IntWritable(vertex.getValue().get()));
			vertex.voteToHalt();
			return;
			}

		//find maximum of incoming labels
		int maxl=Integer.MIN_VALUE;
		for (IntWritable message : messages)
			maxl=Math.max(maxl,message.get());

		//update and propagate if new is more than current
		if (maxl>vertex.getValue().get()) {
			vertex.setValue(new IntWritable(maxl));
			for (Edge <IntWritable,NullWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(),new IntWritable(vertex.getValue().get()));
			}
		vertex.voteToHalt();
		}
	}
