package org.apache.giraph.io.formats.MST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class VV implements Writable {
	public int family;
	public int src;
	public int dst;

	public VV() {
		this.family=-1;
		this.src=-1;
		this.dst=-1;
		}

	public VV(int family,int src,int dst) {
		this.family=family;
		this.src=src;
		this.dst=dst;
		}

	public void write(DataOutput out) throws IOException {
		out.writeInt(family);
		out.writeInt(src);
		out.writeInt(dst);
		}

	public void readFields(DataInput in) throws IOException {
		family=in.readInt();
		src=in.readInt();
		dst=in.readInt();
		}

	@Override
    	public String toString() {
    		return family+"\t"+src+"\t"+dst;
		}
	}
