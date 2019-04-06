package org.apache.giraph.io.formats.PageRank;

import java.util.List;
import org.json.JSONArray;
import java.io.IOException;
import org.json.JSONException;
import org.apache.hadoop.io.Text;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class JsonCustomVertexInputFormat extends TextVertexInputFormat <IntWritable,DoubleWritable,NullWritable> {
	@Override
	public TextVertexReader createVertexReader(InputSplit split,TaskAttemptContext context) {
		return new JsonCustomVertexReader();
		}

	class JsonCustomVertexReader extends TextVertexReaderFromEachLineProcessedHandlingExceptions <JSONArray,JSONException> {
		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
			}

		@Override
		protected IntWritable getId(JSONArray jsonVertex) throws JSONException,IOException {
			return new IntWritable(jsonVertex.getInt(0));
			}

		@Override
		protected DoubleWritable getValue(JSONArray jsonVertex) throws JSONException,IOException {
			return new DoubleWritable(0);
			}

		@Override
		protected Iterable <Edge<IntWritable,NullWritable>> getEdges(JSONArray jsonVertex) throws JSONException,IOException {
			JSONArray jsonEdgeArray=jsonVertex.getJSONArray(1);
			List <Edge<IntWritable,NullWritable>> edges=Lists.newArrayListWithCapacity(jsonEdgeArray.length());
			for (int i=0; i<jsonEdgeArray.length(); ++i)
					edges.add(EdgeFactory.create(new IntWritable(jsonEdgeArray.getInt(i)),NullWritable.get()));
			return edges;
			}

		@Override
		protected Vertex <IntWritable,DoubleWritable,NullWritable> handleException(Text line,JSONArray jsonVertex,JSONException e) {
			throw new IllegalArgumentException("Couldn't get vertex from line "+line,e);
			}
		}
	}
