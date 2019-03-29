package org.apache.giraph.io.formats.PushRelabel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MV implements Writable {
	public int parent;
	public int height;
	public double flow;
	
	public MV() {
		this.parent=0;
		this.height=0;
		this.flow=0;
		}
	
	public MV(int parent,int height,double flow) {
		this.parent=parent;
		this.height=height;
		this.flow=flow;
		}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(parent);
		out.writeInt(height);
		out.writeDouble(flow);
		}
		
	public void readFields(DataInput in) throws IOException {
		parent=in.readInt();				
		height=in.readInt();
		flow=in.readDouble();
		}
	}
