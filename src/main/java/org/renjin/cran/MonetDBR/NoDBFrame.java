package org.renjin.cran.MonetDBR;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.renjin.cran.MonetDBR.ResultSetConverter.RTYPE;
import org.renjin.eval.EvalException;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.DoubleVector;
import org.renjin.sexp.IntVector;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.SEXP;
import org.renjin.sexp.StringVector;
import org.renjin.sexp.Vector;

public class NoDBFrame {

	private ByteBuffer bb = null;
	private FileInputStream fis = null;
	private char fieldsep = 0;
	private String[] header;
	private RTYPE[] types;

	// TODO: opt. header, quotes, newlines, escapes, encodings, ...
	// but not now, this is enough for acs
	public NoDBFrame(String filename, String sep) throws IOException {
		fis = new FileInputStream(filename);
		FileChannel fc = fis.getChannel();
		/* Memory mapping, oh yeah */
		bb = fc.map(MapMode.READ_ONLY, 0, fc.size()).asReadOnlyBuffer();
		bb.position(0);
		this.fieldsep = sep.charAt(0);

		findRowstarts();

		// get column names and types
		header = new String(readLine(0)).split("" + fieldsep);
		types = new RTYPE[header.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = RTYPE.INTEGER;
		}
		sniffrows = Math.min(getRowcount(), sniffrows);
		int read = 1;
		while (read < sniffrows) {
			for (int i = 0; i < types.length; i++) {
				String val = getField(read, i);
				if (intPattern.matcher(val).find()) {
					types[i] = RTYPE.NUMERIC;
				}
				if (floatPattern.matcher(val).find()) {
					types[i] = RTYPE.CHARACTER;
				}
			}
			read++;
		}
	}

	private byte[] readLine(int line) {
		if (line > getRowcount()) {
			throw new EvalException("trying to read line " + line
					+ ", but only have " + rowstarts.size());
		}
		bb.position(rowstarts.get(line));
		int linelen = 0;
		if (line < rowstarts.size() - 1) {
			linelen = rowstarts.get(line + 1) - rowstarts.get(line) - 1;
		} else {
			linelen = bb.remaining();
		}
		byte[] linebuf = new byte[linelen];
		for (int i = 0; i < linelen; i++) {
			linebuf[i] = bb.get();
		}
		return linebuf;
	}

	private int sniffrows = 10;
	private Pattern intPattern = Pattern.compile("[^0-9]");
	private Pattern floatPattern = Pattern.compile("[^0-9.]");

	private final List<Integer> rowstarts = new ArrayList<Integer>();
	private final Map<Integer, int[]> fieldstarts = new HashMap<Integer, int[]>();

	/* do we want to keep this around? probably yes */
	/* we also need this for the vector length, so... */
	private void findRowstarts() throws IOException {
		bb.position(0);
		rowstarts.add(0);
		while (bb.hasRemaining()) {
			if (bb.get() == '\n' && bb.hasRemaining()) {
				rowstarts.add(bb.position());
			}
		}
	}

	public ListVector getDf() throws IOException {

		ListVector.NamedBuilder dfb = new ListVector.NamedBuilder();
		for (int i = 0; i < types.length; i++) {
			switch (types[i]) {
			case INTEGER:
				dfb.add(header[i], new IntNdbVector(i));
				break;
			case CHARACTER:
				dfb.add(header[i], new StringNdbVector(i));
				break;
			case NUMERIC:
				dfb.add(header[i], new DoubleNdbVector(i));
				break;
			default:
				throw new EvalException("unknown type " + types[i]);
			}
		}
		dfb.setAttribute("row.names", new NoDBRownamesVector());
		dfb.setAttribute("class", StringVector.valueOf("data.frame"));
		return dfb.build();
	}

	/* this is evil */
	public String getField(int row, int col) throws IOException {
		if (row > getRowcount()) {
			throw new IOException("file has only " + getRowcount()
					+ " rows, but you want row " + row);
		}
		if (col > types.length) {
			throw new IOException(" We have only " + types.length
					+ " columns, but you want " + col);
		}

		row = row + 1; // because header
		byte[] line = readLine(row);
		if (types.length == 1) { // special case: single-col file
			return new String(line);
		}
		// TODO: limit size of fieldstarts?
		// find offsets of fields within line
		if (!fieldstarts.containsKey(row)) {
			int ccol = 0;
			int[] linepos = new int[types.length - 1];
			for (int i = 0; i < line.length; i++) {
				if (line[i] == fieldsep) {
					linepos[ccol++] = i;
				}
			}
			fieldstarts.put(row, linepos);
		}
		int[] linepos = fieldstarts.get(row);
		// figure out which bytes belong to the field
		int start = 0;
		int readlen = 0;

		if (col == 0) {
			start = rowstarts.get(row);
			readlen = linepos[0];
		} else {
			start = rowstarts.get(row) + linepos[col - 1] + 1;
			if (col < types.length - 1) {
				readlen = linepos[col] - linepos[col - 1] - 1;
			} else {
				if (row < rowstarts.size() - 1) {
					readlen = rowstarts.get(row + 1) - 1 - start;
				} else { // special case for last line
					readlen = -1;
				}
			}
		}
		bb.position(start);
		if (readlen < 0) {
			readlen = bb.remaining() - 1; // newline at end of file
		}
		if (readlen < 1) {
			return "";
		}
		byte[] field = new byte[readlen];
		for (int i = 0; i < readlen; i++) {
			field[i] = bb.get();
		}
		return new String(field);
	}

	public int getRowcount() {
		return rowstarts.size() - 1; // because header
	}

	public static void main(String[] args) throws Exception {
		ListVector df = new NoDBFrame(args[0], ",").getDf();
		System.out.println(((Vector) df.get("AGEP")).getElementAsInt(300));
		System.out.println(((Vector) df.get("PUMA")).getElementAsDouble(400));
		System.out.println(((Vector) df.get("SERIALNO")).getElementAsString(2));
		// System.out.println(((Vector) df.get("a")).getElementAsString(0));
		// System.out.println(((Vector) df.get("b")).getElementAsString(1));
		// System.out.println(((Vector) df.get("c")).getElementAsString(2));
	}

	public void finalize() {
		try {
			fis.close();
		} catch (IOException e) {
		}
	}

	
	// TODO: cache the parsed values in a dynamically growing int array?
	private class IntNdbVector extends IntVector {
		private int col;

		public IntNdbVector(int col) {
			this.col = col;
		}

		public boolean isConstantAccessTime() {
			return false;
		}

		@Override
		public int length() {
			return NoDBFrame.this.getRowcount();
		}

		@Override
		public int getElementAsInt(int i) {
			try {
				String sval = NoDBFrame.this.getField(i, col);
				return "".equals(sval) ? IntVector.NA : Integer.parseInt(sval);
			} catch (Exception e) {
				throw new EvalException(e);
			}
		}

		@Override
		public boolean isElementNA(int i) {
			try {
				return "".equals(NoDBFrame.this.getField(i, col));
			} catch (Exception e) {
				throw new EvalException(e);
			}
		}

		@Override
		protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
			throw new EvalException("no clone");
		}
	}

	@SuppressWarnings("unchecked")
	private class DoubleNdbVector extends DoubleVector {
		private int col;

		public DoubleNdbVector(int col) {
			this.col = col;
		}

		public boolean isConstantAccessTime() {
			return false;
		}

		@Override
		public int length() {
			return NoDBFrame.this.getRowcount();
		}

		@Override
		protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
			throw new EvalException("no clone");
		}

		@Override
		public double getElementAsDouble(int i) {
			try {
				String sval = NoDBFrame.this.getField(i, col);
				return "".equals(sval) ? DoubleVector.NA : Double
						.parseDouble(sval);
			} catch (Exception e) {
				throw new EvalException(e);
			}
		}

		@Override
		public boolean isElementNA(int i) {
			try {
				return "".equals(NoDBFrame.this.getField(i, col));
			} catch (Exception e) {
				throw new EvalException(e);
			}
		}
	}

	private class StringNdbVector extends StringVector {
		private int col;

		public StringNdbVector(int col) {
			super(AttributeMap.EMPTY);
			this.col = col;
		}

		public boolean isConstantAccessTime() {
			return false;
		}

		@Override
		public int length() {
			return NoDBFrame.this.getRowcount();
		}

		public String getElementAsString(int i) {
			try {
				return NoDBFrame.this.getField(i, col);
			} catch (Exception e) {
				throw new EvalException(e);
			}
		}

		@Override
		protected StringVector cloneWithNewAttributes(AttributeMap attributes) {
			throw new EvalException("no clone");
		}

		@Override
		public boolean isElementNA(int i) {
			return false;
		}
	}

	private class NoDBRownamesVector extends IntVector {
		public boolean isConstantAccessTime() {
			return false;
		}

		@Override
		public int length() {
			return NoDBFrame.this.getRowcount();
		}

		@Override
		public int getElementAsInt(int i) {
			/* haha, have your int back */
			return i;
		}

		@Override
		protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
			throw new EvalException("U NO CLONE ME");
		}
	}

}
