package org.apache.giraph.io.formats.MST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MV implements Writable {
	public int val;
	public int src;
	public int dst;
	public int tag;

	public MV() {
		this.val=-1;
		this.src=-1;
		this.dst=-1;
		this.tag=-1;
		}

	public MV(int val,int src,int dst,int tag) {
		this.val=val;
		this.src=src;
		this.dst=dst;
		this.tag=tag;
		}

	public void write(DataOutput out) throws IOException {
		out.writeInt(val);
		out.writeInt(src);
		out.writeInt(dst);
		out.writeInt(tag);
		}

	public void readFields(DataInput in) throws IOException {
		val=in.readInt();
		src=in.readInt();
		dst=in.readInt();
		tag=in.readInt();
		}
	}
