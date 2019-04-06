package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.graph.BasicComputation;

@Algorithm(name="Single Source Shortest Path",description="Find shortest path from 0 to every other vertex")

public class SSSP extends BasicComputation <IntWritable,DoubleWritable,DoubleWritable,DoubleWritable> {
	@Override
	public void compute(Vertex <IntWritable,DoubleWritable,DoubleWritable> vertex,Iterable <DoubleWritable> messages) throws IOException {
		//initialize and send source distances
		if (getSuperstep()==0) {
			if (vertex.getId().get()==0) {
				vertex.setValue(new DoubleWritable(0));
				for (Edge <IntWritable,DoubleWritable> edge : vertex.getEdges())
					sendMessage(edge.getTargetVertexId(),new DoubleWritable(edge.getValue().get()));
				}	
			else
				vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
			vertex.voteToHalt();
			}

		//find minimum of incoming updates
		double mind=Double.MAX_VALUE;
		for (DoubleWritable message : messages)
			mind=Math.min(mind,message.get());

		//update and propagate if new is less than current
		if (mind<vertex.getValue().get()) {
			vertex.setValue(new DoubleWritable(mind));
			for (Edge <IntWritable,DoubleWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(),new DoubleWritable(mind+edge.getValue().get()));
			}
		vertex.voteToHalt();
		}
	}
