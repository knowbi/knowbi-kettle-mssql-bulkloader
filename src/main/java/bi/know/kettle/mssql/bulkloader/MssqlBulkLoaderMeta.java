package bi.know.kettle.mssql.bulkloader;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;
import java.util.List;
import java.util.Map;

@Step(id = "MssqlBulkLoader",
        image = "MssqlBulkLoader.svg",
        i18nPackageName = "bi.know.kettle.mssql.bulkloader",
        name = "MssqlBulkLoader.Step.Name",
        description = "MssqlBulkLoader.Step.Description",
        categoryDescription = "MssqlBulkLoader.Step.Category",
        isSeparateClassLoaderNeeded = true
)
public class MssqlBulkLoaderMeta extends BaseStepMeta implements StepMetaInterface {
    private static Class<?> PKG = MssqlBulkLoader.class; // for i18n purposes, needed by Translator2!!

    private DatabaseMeta databaseMeta;
    private String schemaName;
    private String tableName;
    private String batchSize;
    private boolean truncate;
    private boolean specifyDatabaseFields;
    private String[] databaseFields;
    private String[] streamFields;


    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {

        return new MssqlBulkLoader(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    public StepDataInterface getStepData() {

        return new MssqlBulkLoaderData();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {

        return new MssqlBulkLoaderDialog(shell, meta, transMeta, name);
    }

    public void setDefault() {
        databaseMeta = null;
        schemaName="";
        tableName = "";
        batchSize = "100000";
        truncate = true;
        specifyDatabaseFields=false;
        allocate(0);
    }

    public String getXML() throws KettleException{
        String retval = "";
        retval+="<connection>" + databaseMeta.getName() + "</connection>" + Const.CR;
        retval+="<schemaName>" + schemaName + "</schemaName>" + Const.CR;
        retval+="<tableName>" + tableName + "</tableName>" + Const.CR;
        retval+="<batchSize>" + batchSize + "</batchSize>" + Const.CR;
        retval+="<truncate>" + truncate + "</truncate>" + Const.CR;
        retval+="<specifyDatabaseFields>" + specifyDatabaseFields + "</specifyDatabaseFields>" + Const.CR;
        retval+="<fields>" + Const.CR;
        for(int i = 0; i<databaseFields.length;i++){
            retval+="<field>" + Const.CR;
            retval+="<database_field>" + databaseFields[i] + "</database_field>" + Const.CR;
            retval+="<stream_field>" + streamFields[i] + "</stream_field>" + Const.CR;
            retval+="</field>" + Const.CR;
        }
        retval+="</fields>" + Const.CR;
        return retval;
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException{
        String con = XMLHandler.getTagValue( stepnode, "connection" );
        databaseMeta = DatabaseMeta.findDatabase( databases, con );
        schemaName = XMLHandler.getTagValue(stepnode,"schemaName");
        tableName = XMLHandler.getTagValue(stepnode,"tableName");
        batchSize = XMLHandler.getTagValue(stepnode,"batchSize");
        truncate = Boolean.valueOf(XMLHandler.getTagValue(stepnode,"truncate"));
        specifyDatabaseFields = Boolean.valueOf(XMLHandler.getTagValue(stepnode,"specifyDatabaseFields"));

        Node fields = XMLHandler.getSubNode(stepnode,"fields");
        int nrFields = XMLHandler.countNodes(fields,"field");

        allocate(nrFields);

        for(int i=0 ; i< nrFields;i++){
            Node field = XMLHandler.getSubNodeByNr(fields,"field",i);
            databaseFields[i] = XMLHandler.getTagValue(field,"database_field");
            streamFields[i] = XMLHandler.getTagValue(field,"stream_field");
        }

    }


    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
    {

    }

    public Object clone()
    {
        Object retval = super.clone();
        return retval;
    }

    public void allocate(int nbFields){
        databaseFields = new String[nbFields];
        streamFields = new String[nbFields];
    }

    public DatabaseMeta getDatabaseMeta() {
        return databaseMeta;
    }

    public void setDatabaseMeta( DatabaseMeta database ) {
        this.databaseMeta = database;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public void setTruncate(boolean truncate) {
        this.truncate = truncate;
    }

    public boolean isSpecifyDatabaseFields() {
        return specifyDatabaseFields;
    }

    public void setSpecifyDatabaseFields(boolean specifyDatabaseFields) {
        this.specifyDatabaseFields = specifyDatabaseFields;
    }

    public String[] getStreamFields() {
        return streamFields;
    }

    public void setStreamFields(String[] streamFields) {
        this.streamFields = streamFields;
    }

    public String[] getDatabaseFields() {
        return databaseFields;
    }

    public void setDatabaseFields(String[] databaseFields) {
        this.databaseFields = databaseFields;
    }
}