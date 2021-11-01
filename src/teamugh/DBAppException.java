package teamugh;

@SuppressWarnings("serial")
public class DBAppException extends Exception {

	public DBAppException()
	{
		super();
	}

	public DBAppException( String e)
	{
		super(e);
	}
}