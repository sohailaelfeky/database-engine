package teamugh;

import java.io.Serializable;

public class SQLTerm implements Serializable {

	private static final long serialVersionUID = 1L;
	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue;
	
	public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
		this._strTableName = _strTableName;
		this._strColumnName = _strColumnName;
		this._strOperator = _strOperator;
		this._objValue = _objValue;
	}
}