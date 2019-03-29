package org.apache.giraph.io.formats.PushRelabel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class VV implements Writable {
	public int height;
	public double excess;
	
	public VV() {
		this.height=0;
		this.excess=0;
		}
	
	public VV(int height,double excess) {
		this.height=height;
		this.excess=excess;
		}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(height);
		out.writeDouble(excess);
		}
			
	public void readFields(DataInput in) throws IOException {
		height=in.readInt();
		excess=in.readDouble();
		}
	
	@Override
    public String toString() {
    	return excess+"\t"+height;
		}
	}
