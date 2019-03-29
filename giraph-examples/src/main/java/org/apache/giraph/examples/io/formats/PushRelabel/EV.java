package org.apache.giraph.io.formats.PushRelabel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class EV implements Writable {
	public double capacity;
	public int height;
	
	public EV() {
		this.capacity=0;
		this.height=0;
		}
	
	public EV(double capacity,int height) {
		this.capacity=capacity;
		this.height=height;
		}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(capacity);
		out.writeInt(height);
		}
			
	public void readFields(DataInput in) throws IOException {
		capacity=in.readDouble();
		height=in.readInt();
		}
	}
