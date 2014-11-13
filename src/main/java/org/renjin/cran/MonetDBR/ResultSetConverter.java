package org.renjin.cran.MonetDBR;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.renjin.eval.EvalException;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.IntArrayVector;
import org.renjin.sexp.ListVector;
import org.renjin.sexp.LogicalArrayVector;
import org.renjin.sexp.StringArrayVector;
import org.renjin.sexp.StringVector;
import org.renjin.sexp.Vector;
import org.renjin.sexp.Vector.Builder;

public class ResultSetConverter {
	public static boolean hasCompleted(ResultSet rs) {
		try {
			return (rs.isClosed() || rs.isAfterLast());
		} catch (SQLException e) {
			return false;
		}
	}

	public static ListVector columnInfo(ResultSet rs) {
		try {
			ListVector.Builder tv = new ListVector.Builder();
			ResultSetMetaData rsm = rs.getMetaData();
			for (int i = 1; i < rsm.getColumnCount() + 1; i++) {
				ListVector.NamedBuilder cv = new ListVector.NamedBuilder();
				cv.add("name", rsm.getColumnName(i));
				cv.add("type", rsm.getColumnTypeName(i));
				tv.add(cv.build());
			}
			return tv.build();
		} catch (SQLException e) {
			throw new EvalException(e);
		}
	}

	// TODO: consider blob -> raw, too? bit esoteric.
	public static enum RTYPE {
		INTEGER, NUMERIC, CHARACTER, LOGICAL
	};

	public static ListVector fetch(ResultSet rs, long n) {
		try {
			if (n < 0) {
				n = Long.MAX_VALUE;
			}
			ListVector ti = columnInfo(rs);
			/* cache types, we need to look this up for *every* value */
			RTYPE[] rtypes = new RTYPE[ti.length()];
			/* column builders */
			Map<Integer, Builder<Vector>> builders = new HashMap<Integer, Builder<Vector>>();
			for (int i = 0; i < ti.length(); i++) {
				ListVector ci = (ListVector) ti.get(i);
				String tpe = ci.get("type").asString();
				rtypes[i] = null;
				if (tpe.endsWith("int") || tpe.equals("wrd")) {
					// TODO: long values?
					builders.put(i, new IntArrayVector.Builder());
					rtypes[i] = RTYPE.INTEGER;
				}
				if (tpe.equals("decimal") || tpe.equals("real")
						|| tpe.equals("double")) {
					builders.put(i, new DoubleArrayVector.Builder());
					rtypes[i] = RTYPE.NUMERIC;
				}
				if (tpe.equals("boolean")) {
					builders.put(i, new LogicalArrayVector.Builder());
					rtypes[i] = RTYPE.LOGICAL;
				}
				if (tpe.equals("clob") || tpe.equals("varchar")
						|| tpe.equals("char") || tpe.equals("date")
						|| tpe.equals("time")) {
					builders.put(i, new StringArrayVector.Builder());
					rtypes[i] = RTYPE.CHARACTER;
				}
				if (rtypes[i] == null) {
					throw new EvalException("Unknown column type " + ci);
				}
			}

			int ival;
			double nval;
			boolean lval;
			String cval;
			long rows = 0;
			/* collect values */
			while (n > 0 && rs.next()) {
				rows++;
				for (int i = 0; i < rtypes.length; i++) {
					Builder<Vector> bld = builders.get(i);
					switch (rtypes[i]) {
					case INTEGER:
						/* Behold the beauty of JDBC */
						ival = rs.getInt(i + 1);
						if (rs.wasNull()) {
							bld.addNA();
						} else {
							((IntArrayVector.Builder) bld).add(ival);
						}
						break;
					case NUMERIC:
						nval = rs.getDouble(i + 1);
						if (rs.wasNull()) {
							bld.addNA();
						} else {
							((DoubleArrayVector.Builder) bld).add(nval);
						}
						break;
					case LOGICAL:
						lval = rs.getBoolean(i + 1);
						if (rs.wasNull()) {
							bld.addNA();
						} else {
							((LogicalArrayVector.Builder) bld).add(lval);
						}
						break;
					case CHARACTER:
						cval = rs.getString(i + 1);
						if (rs.wasNull()) {
							bld.addNA();
						} else {
							((StringArrayVector.Builder) bld).add(cval);
						}
						break;
					}
				}
				n--;
			}
			/* call build() on each column and add them as named cols to df */
			ListVector.NamedBuilder dfb = new ListVector.NamedBuilder();
			for (int i = 0; i < ti.length(); i++) {
				ListVector ci = (ListVector) ti.get(i);
				dfb.add(ci.get("name").asString(), builders.get(i).build());
			}
			/* I'm a data.frame object */
			IntArrayVector.Builder rnb = new IntArrayVector.Builder();
			for (long i = 0; i < rows; i++) {
				rnb.add(i);
			}
			dfb.setAttribute("row.names", rnb.build());
			dfb.setAttribute("class", StringVector.valueOf("data.frame"));
			ListVector lv = dfb.build();
			return lv;
		} catch (SQLException e) {
			throw new EvalException(e);
		}
	}
}
