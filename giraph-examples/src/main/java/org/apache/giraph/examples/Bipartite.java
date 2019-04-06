package org.apache.giraph.examples;

import java.util.Random;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.giraph.graph.BasicComputation;

@Algorithm(name="Bipartite Matching",description="Randomized MCBM algorithm")

public class Bipartite extends BasicComputation <IntWritable,IntWritable,NullWritable,IntWritable> {
	@Override
	public void compute(Vertex <IntWritable,IntWritable,NullWritable> vertex,Iterable <IntWritable> messages) throws IOException {
		if (getSuperstep()==0) vertex.setValue(new IntWritable(-1));

		int id=vertex.getId().get();
		int phase=(int)getSuperstep()%4;
		boolean isleft=id<getTotalNumVertices()/2;
		boolean matched=vertex.getValue().get()>=0;

		if (phase==0 && isleft && !matched) {
			for (Edge <IntWritable,NullWritable> edge : vertex.getEdges())
				sendMessage(edge.getTargetVertexId(),new IntWritable(id));

			vertex.voteToHalt();
			}

		if (phase==1 && !isleft && !matched) {
			int sz=0;
			Random rand=new Random();
			for (IntWritable message : messages) sz++;
			int match=rand.nextInt(sz);

			int i=0;
			for (IntWritable message : messages) {
				sendMessage(new IntWritable(message.get()),new IntWritable(i==match ? id : -id));
				i++;
				}

			vertex.voteToHalt();
			}

		if (phase==2 && isleft && !matched) {
			int yes=0;
			for (IntWritable message : messages)
				if (message.get()>0) yes++;

			if (yes==0) return;

			Random rand=new Random();
			int match=rand.nextInt(yes);

			int i=0;
			for (IntWritable message : messages) if (message.get()>0) {
				if (i==match) {
					vertex.setValue(new IntWritable(message.get()));
					sendMessage(new IntWritable(message.get()),new IntWritable(id));
					}
				i++;
				}

			vertex.voteToHalt();
			}

		if (phase==3 && !isleft) {
			for (IntWritable message : messages)
				vertex.setValue(new IntWritable(message.get()));

			vertex.voteToHalt();
			}
		}
	}

