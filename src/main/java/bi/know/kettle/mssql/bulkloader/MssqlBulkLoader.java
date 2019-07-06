package bi.know.kettle.mssql.bulkloader;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.microsoft.sqlserver.jdbc.*;



public class MssqlBulkLoader  extends BaseStep implements StepInterface {
    private static Class<?> PKG = MssqlBulkLoader.class; // for i18n purposes, needed by Translator2!!


    private MssqlBulkLoaderMeta meta;
    private MssqlBulkLoaderData data;
    private Connection connection;
    private String destinationTable ="";


    private int nbRows,nbRowsBatch;

    private HashMap<String, FieldMeta> databaseFieldMeta = new HashMap();


    private InputStream inputStream = null;
    private StringBuilder stringBuilder;

    private List<Integer> fieldIndexes = new ArrayList<>();

    private String delimiter = ",§;";


    public MssqlBulkLoader(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
        super(s,stepDataInterface,c,t,dis);
    }

    public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        meta = (MssqlBulkLoaderMeta) smi;
        data = (MssqlBulkLoaderData) sdi;

        Object[] r = getRow(); // get row, set busy!


        if (first) {
            //TODO: refactor first
            first = false;

            nbRows = 0;
            nbRowsBatch=0;

            data.outputRowMeta = (RowMetaInterface)getInputRowMeta().clone();

            data.db = new Database(this,meta.getDatabaseMeta());
            data.db.connect();
            connection = data.db.getConnection();
            destinationTable=meta.getSchemaName()+"."+meta.getTableName();

            //TODO: add check if driver version supports bulkload



            try (Statement stmt = connection.createStatement()) {

                //truncate table
                if(meta.isTruncate()) {

                    stmt.executeUpdate("TRUNCATE TABLE " + destinationTable);

                }

                stringBuilder = new StringBuilder();

                String metadataQuery = "select top 1 * FROM "+destinationTable;
                ResultSet metadataResult = stmt.executeQuery(metadataQuery);
                ResultSetMetaData tableDefinition =  metadataResult.getMetaData();

                int colCount = tableDefinition.getColumnCount();
                for(int i=1;i<=colCount;i++){
                    FieldMeta fieldMeta = new FieldMeta(tableDefinition.getColumnName(i),tableDefinition.getColumnType(i),tableDefinition.getPrecision(i),tableDefinition.getScale(i));
                    databaseFieldMeta.put(tableDefinition.getColumnName(i),fieldMeta);
                }


                if(meta.isSpecifyDatabaseFields()){
                    for(int i=0; i<meta.getDatabaseFields().length;i++){
                        int fieldPos =  data.outputRowMeta.indexOfValue(meta.getStreamFields()[i]);
                        FieldMeta fieldMeta = databaseFieldMeta.get(meta.getDatabaseFields()[i]);

                        if(fieldMeta != null && fieldPos >= 0) {
                            fieldMeta.setFieldPosition(fieldPos+1);
                            fieldIndexes.add(fieldPos);
                        } else {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + fieldMeta.getFieldName());
                        }
                    }
                }else{
                    //if fields are not specified do mapping
                    for(int i=0 ; i<data.outputRowMeta.size();i++){
                        ValueMetaInterface valueMeta =  data.outputRowMeta.getValueMeta(i);
                        FieldMeta fieldMeta = databaseFieldMeta.get(valueMeta.getName());

                        if(fieldMeta != null){
                            //fieldposition is 1 based
                            fieldMeta.setFieldPosition(i+1);
                            fieldIndexes.add(i);
                        } else {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + valueMeta.getName());
                        }
                    }
                }

                stmt.close();


            }catch(Exception e){
                stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.GeneralError") + e.getMessage());
            }

        }

        if ( r == null ) {
            // no more input to be expected...

            inputStream = new ByteArrayInputStream(stringBuilder.toString().getBytes());
            stringBuilder.setLength(0);
            executeBatch(inputStream);
            setOutputDone();
            try {
                connection.close();
            } catch (SQLException e) {
                stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.GeneralError") + e.getMessage());
            }
            return false;
        }else {

            //if batch length is reached push it to the database

            if(nbRowsBatch==Integer.parseInt(meta.getBatchSize())){

                inputStream = new ByteArrayInputStream(stringBuilder.toString().getBytes());
                stringBuilder.setLength(0);
                executeBatch(inputStream);
                nbRowsBatch=0;
            }

            //create new line
            //check if only 1 field then the delimiter is not needed
            if(fieldIndexes.size()==1){
                stringBuilder.append(r[fieldIndexes.get(0)].toString()+"\n");
            }else
            {
                for(int index : fieldIndexes){
                    stringBuilder.append(r[index].toString()+delimiter);
                }
                stringBuilder.append("\n");
            }

            nbRows++;
            nbRowsBatch++;
            return true;
        }

    }


    public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
        meta = (MssqlBulkLoaderMeta) smi;
        data = (MssqlBulkLoaderData) sdi;


        if(Utils.isEmpty(getInputRowSets())){
            logError(BaseMessages.getString(PKG, "MssqlBulkLoader.ErrorNoInputRows"));
            return false;
        }


        return super.init(smi, sdi);
    }


    private boolean executeBatch(InputStream inputStream){

        try (   Statement stmt = connection.createStatement();
                SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {


            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(inputStream, StandardCharsets.UTF_8.name(), delimiter, false);

            SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
            copyOptions.setTableLock(true);

            bulkCopy.setBulkCopyOptions(copyOptions);
            bulkCopy.setDestinationTableName(destinationTable);


                //add inputfile metadata and table column mapping
                for (FieldMeta fieldmeta : databaseFieldMeta.values()) {
                    if (fieldmeta.getFieldPosition() != null) {
                        fileRecord.addColumnMetadata(fieldmeta.getFieldPosition(), fieldmeta.getFieldName(), fieldmeta.getFieldType(), fieldmeta.getPrecision(), fieldmeta.getScale());
                        bulkCopy.addColumnMapping(fieldmeta.getFieldName(), fieldmeta.getFieldName());
                    }
                }

            bulkCopy.writeToServer(fileRecord);

            stmt.close();
            bulkCopy.close();

        }catch (Exception e){
            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.GeneralError") + e.getMessage());
            return false;
        }

        setLinesWritten(nbRows);
        return true;

    }

    private boolean stopStep(String errorMessage) {
        logError(errorMessage);
        setErrors(1);
        stopAll();
        setOutputDone();
        return false;
    }


}
