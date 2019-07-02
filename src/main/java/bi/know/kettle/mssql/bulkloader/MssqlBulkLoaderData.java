package bi.know.kettle.mssql.bulkloader;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class MssqlBulkLoaderData extends BaseStepData implements StepDataInterface{
    public RowMetaInterface outputRowMeta;

    public Database db;

    public MssqlBulkLoaderData(){
        super();

        db=null;
    }

}
