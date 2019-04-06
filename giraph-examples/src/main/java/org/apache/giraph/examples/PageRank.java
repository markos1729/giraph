package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.graph.BasicComputation;

@Algorithm(name="Page Rank",description="Update ranks until convergence")

public class PageRank extends BasicComputation <IntWritable,DoubleWritable,NullWritable,DoubleWritable> {
	@Override
	public void compute(Vertex <IntWritable,DoubleWritable,NullWritable> vertex,Iterable <DoubleWritable> messages) throws IOException {
		//initialize ranks
		if (getSuperstep()==0)
			vertex.setValue(new DoubleWritable(1.0/getTotalNumVertices()));

		//find sum of incoming ranks
		double sum=0;
		for (DoubleWritable message : messages)
			sum+=message.get();

		//update tentative page rank
		double prer=vertex.getValue().get();
		double newr=0.15/getTotalNumVertices()+0.85*sum;

		//send new rank / number of edges to neighbors
		if (prer!=newr && getSuperstep()<=50) {
			vertex.setValue(new DoubleWritable(newr));
			for (Edge <IntWritable,NullWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(),new DoubleWritable(newr/vertex.getNumEdges()));
			}
		vertex.voteToHalt();
		}
	}
