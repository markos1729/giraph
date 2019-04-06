package org.apache.giraph.io.formats.MST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class EV implements Writable {
	public int cost;
	public boolean taken;

	public EV() {
		this.cost=0;
		this.taken=false;
		}

	public EV(int cost,boolean taken) {
		this.cost=cost;
		this.taken=taken;
		}

	public void write(DataOutput out) throws IOException {
		out.writeInt(cost);
		out.writeBoolean(taken);
		}

	public void readFields(DataInput in) throws IOException {
		cost=in.readInt();
		taken=in.readBoolean();
		}
	}
