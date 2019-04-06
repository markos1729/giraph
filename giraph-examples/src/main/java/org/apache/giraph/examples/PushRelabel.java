package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.io.formats.PushRelabel.VV;
import org.apache.giraph.io.formats.PushRelabel.EV;
import org.apache.giraph.io.formats.PushRelabel.MV;

@Algorithm(name="Goldberg & Tarjan Push Relabel",description="Find max flow from source 0 to sink 1")

public class PushRelabel extends BasicComputation <IntWritable,VV,EV,MV> {
	@Override
	public void compute(Vertex <IntWritable,VV,EV> vertex,Iterable <MV> messages) throws IOException {
		vertex.setValue(new VV(1,5));
		//if (getSuperstep()==0) {
		//	if (vertex.getId().get()==0) {
				//send initial flow from source together with its id height
				//int height=vertex.getValue().height;
		//		for (Edge <IntWritable,EV> edge : vertex.getEdges()) {
					//sendMessage(edge.getTargetVertexId(),new MV(0,height,edge.getValue().capacity));
		//			vertex.setEdgeValue(edge.getTargetVertexId(),new EV(0,0));
		//			}
		//		}
			vertex.voteToHalt();
		//	}
		//else vertex.voteToHalt();
		/*else {
			int height=vertex.getValue().height;
			double excess=vertex.getValue().excess;

			//receive incoming flow and update excess flow
			for (MV message : messages) {
				double in=message.flow;
				IntWritable parent=new IntWritable(message.parent);
				//update backward edge = important for canceling flow
				vertex.setEdgeValue(parent,new EV(vertex.getEdgeValue(parent).capacity+in,message.height));
				excess+=in;
				}

			//source and sink are never overflowed = halt immediately
			if (vertex.getId().get()<=1) {
				vertex.setValue(new VV(height,excess));
				vertex.voteToHalt();
				return;
				}

			boolean relabel=true;
			int lowest=Integer.MAX_VALUE;
			//if must relabel, find the lowest id of its neighbors with positive capacity

			if (excess>0)
				for (Edge <IntWritable,EV> edge : vertex.getEdges()) {
					double capacity=edge.getValue().capacity;
					int neighbor_height=edge.getValue().height;

					if (capacity>0) {
						//send flow through this edge
						if (height==neighbor_height+1) {
							//update excess
							double delta=Math.min(excess,capacity);
							excess-=delta;
							capacity-=delta;
							//update edge value and send message to neighbor
							vertex.setEdgeValue(edge.getTargetVertexId(),new EV(capacity,neighbor_height));
							sendMessage(edge.getTargetVertexId(),new MV(vertex.getId().get(),height,delta));
							}

						if (excess==0) { relabel=false; break; }
						//cannot send any more flow
						
						if (capacity>0) {
							//relabel, if for all neighbors with positive capacity: height<=neighbor.height
							if (height>neighbor_height) { relabel=false; break; }
							else lowest=Math.min(lowest,neighbor_height+1);
							}
						}
					}
			else relabel=false;
			
			if (relabel) {
				//update height and broadcast to neighbors
				height=lowest;
				for (Edge <IntWritable,EV> edge : vertex.getEdges())
					sendMessage(edge.getTargetVertexId(),new MV(vertex.getId().get(),height,0));
				}

			//update height and excess flow
			vertex.setValue(new VV(height,excess));
			if (excess==0) vertex.voteToHalt();
			}*/
		}
	}
