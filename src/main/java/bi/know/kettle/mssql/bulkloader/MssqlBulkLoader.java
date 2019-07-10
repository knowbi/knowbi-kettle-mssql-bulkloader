package bi.know.kettle.mssql.bulkloader;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
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
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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

    //fieldindex, fieldmeta
    private HashMap<Integer, FieldMeta> fieldMapping = new HashMap();

    private InputStream inputStream = null;
    private StringBuilder stringBuilder;

    private List<ValueMetaInterface> inputMeta;

    private String delimiter = ",§;";

    private Integer batchSize;




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

            inputMeta = getInputRowMeta().getValueMetaList();
            data.outputRowMeta = getInputRowMeta().clone();

            try {
                batchSize = Integer.parseInt(environmentSubstitute(meta.getBatchSize()));
                if(batchSize<0){
                    batchSize=100000;
                }
            } catch (Exception e){
                stopStep("Batchsize must be Integer value");
            }

            data.db = new Database(this,meta.getDatabaseMeta());
            data.db.connect();
            connection = data.db.getConnection();
            if(Utils.isEmpty(environmentSubstitute(meta.getSchemaName()))){
                destinationTable= environmentSubstitute(meta.getTableName());
            } else {
                destinationTable= environmentSubstitute(meta.getSchemaName())+"."+environmentSubstitute(meta.getTableName());
            }


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

                int index =1;
                if(meta.isSpecifyDatabaseFields()) {
                    for (int i = 0; i < meta.getDatabaseFields().length; i++) {
                        int fieldPos = data.outputRowMeta.indexOfValue(meta.getStreamFields()[i]);
                        FieldMeta fieldMeta = null;

                        for (int ii = 1; ii <= tableDefinition.getColumnCount(); ii++) {
                            if (meta.getDatabaseFields()[i].equals(tableDefinition.getColumnName(ii))) {
                                fieldMeta = new FieldMeta(index, tableDefinition.getColumnName(ii), tableDefinition.getColumnType(ii), tableDefinition.getPrecision(ii), tableDefinition.getScale(ii));
                                fieldMapping.put(fieldPos, fieldMeta);

                            }
                        }

                        if(inputMeta.get(fieldPos) instanceof ValueMetaDate)
                        {
                            ValueMetaDate valueMetaDate = (ValueMetaDate) inputMeta.get(fieldPos);
                            fieldMapping.get(fieldPos).setDateFormat(valueMetaDate.getDateFormat());
                            fieldMapping.get(fieldPos).setDateTimeFormatter(DateTimeFormatter.ofPattern(valueMetaDate.getDateFormat().toPattern()));

                        }

                        index++;
                        if (fieldMeta == null || fieldPos < 0) {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + meta.getDatabaseFields()[i]);
                            break;
                        }
                    }
                } else {
                    //if fields are not specified do mapping
                    for(int i=0 ; i<data.outputRowMeta.size();i++){
                        ValueMetaInterface valueMeta =  data.outputRowMeta.getValueMeta(i);
                        FieldMeta fieldMeta = null;

                        for (int ii = 1; ii <= tableDefinition.getColumnCount(); ii++) {
                            if (valueMeta.getName().equals(tableDefinition.getColumnName(ii))) {
                                fieldMeta = new FieldMeta(i+1, tableDefinition.getColumnName(ii), tableDefinition.getColumnType(ii), tableDefinition.getPrecision(ii), tableDefinition.getScale(ii));
                                fieldMapping.put(i, fieldMeta);

                            }
                        }

                        if(inputMeta.get(i) instanceof ValueMetaDate)
                        {
                            ValueMetaDate valueMetaDate = (ValueMetaDate) inputMeta.get(i);
                            fieldMapping.get(i).setDateFormat(valueMetaDate.getDateFormat());
                            fieldMapping.get(i).setDateTimeFormatter(DateTimeFormatter.ofPattern(valueMetaDate.getDateFormat().toPattern()));

                        }

                        if(fieldMeta == null){
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + valueMeta.getName());
                            break;
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

            if(nbRowsBatch==batchSize){

                inputStream = new ByteArrayInputStream(stringBuilder.toString().getBytes());
                stringBuilder.setLength(0);
                executeBatch(inputStream);
                nbRowsBatch=0;
            }


            int index=0;

            for(int fieldPos : fieldMapping.keySet()){
                String value;

                if(inputMeta.get(fieldPos) instanceof ValueMetaDate){
                    value = convertDate(r[fieldPos], fieldMapping.get(fieldPos));

                } else {
                    value = r[fieldPos].toString();

                }


                if(index==0){
                    stringBuilder.append(value);

                } else {
                    stringBuilder.append(delimiter + value);

                }
                index++;

            }
            stringBuilder.append("\n");

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
                for (FieldMeta fieldmeta : fieldMapping.values()) {
                    if (fieldmeta.getFieldPosition() != null) {
                        fileRecord.addColumnMetadata(fieldmeta.getFieldPosition(), fieldmeta.getFieldName(), fieldmeta.getFieldType(), fieldmeta.getPrecision(), fieldmeta.getScale(),fieldmeta.getDateTimeFormatter());
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

    private String convertDate(Object dateValue, FieldMeta dateFieldMeta){
        SimpleDateFormat simpleDateFormat = dateFieldMeta.getDateFormat();
        String date = simpleDateFormat.format((Date) dateValue);
        return date;
    }



}
