package bi.know.kettle.mssql.bulkloader;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
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
import java.util.LinkedHashMap;
import java.util.List;
import com.microsoft.sqlserver.jdbc.*;


public class MssqlBulkLoader extends BaseStep implements StepInterface {
    private static Class<?> PKG = MssqlBulkLoader.class; // for i18n purposes, needed by Translator2!!

    private MssqlBulkLoaderMeta meta;
    private MssqlBulkLoaderData data;
    private Connection connection;
    private String destinationTable = "";

    private int nbRows, nbRowsBatch;

    //fieldindex, fieldmeta
    private LinkedHashMap<Integer, FieldMeta> fieldMapping = new LinkedHashMap();

    private StringBuilder stringBuilder;

    private List<ValueMetaInterface> inputMeta;

    private String delimiter = ",§;";

    private Integer batchSize;

    private Statement stmt;
    private SQLServerBulkCopy bulkCopy;
    private Boolean firstBatch = true;


    public MssqlBulkLoader(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
        super(s, stepDataInterface, c, t, dis);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (MssqlBulkLoaderMeta) smi;
        data = (MssqlBulkLoaderData) sdi;

        //open database connection

        if(first) {
            data.db = new Database(this, meta.getDatabaseMeta());
            data.db.connect();
            connection = data.db.getConnection();
        }

        //stop if no data, done to avoid nullpointer with valuemeta
        Object[] r = getRow(); // get row, set busy!
        if (r == null) {
            // no more input to be expected...
            //truncate table if there are no rows
            if ( first && meta.isTruncate() ) {
                truncateTable();
            }
            return false;
        }

        if (first) {
            first = false;

            nbRows = 0;
            nbRowsBatch = 0;

            data.outputRowMeta = getInputRowMeta().clone();


            try {
                batchSize = Integer.parseInt(environmentSubstitute(meta.getBatchSize()));
                if (batchSize < 0) {
                    batchSize = 100000;
                }
            } catch (Exception e) {
                stopStep("Batchsize must be Integer value");
            }


            if (Utils.isEmpty(environmentSubstitute(meta.getSchemaName()))) {
                destinationTable = environmentSubstitute(meta.getTableName());
            } else {
                destinationTable = environmentSubstitute(meta.getSchemaName()) + "." + environmentSubstitute(meta.getTableName());
            }


            try {

                inputMeta = getInputRowMeta().getValueMetaList();
                stmt = connection.createStatement();
                bulkCopy = new SQLServerBulkCopy(connection);

                //truncate table
                if (meta.isTruncate()) {
                    truncateTable();
                }

                //Instantiate Stringbuilder
                stringBuilder = new StringBuilder();

                String metadataQuery = "select top 1 * FROM " + destinationTable;
                ResultSet metadataResult = stmt.executeQuery(metadataQuery);
                ResultSetMetaData tableDefinition = metadataResult.getMetaData();

                //index used in file position for bulkload
                int index = 1;

                if (meta.isSpecifyDatabaseFields()) {

                    //search for every field added in the step
                    for (int i = 0; i < meta.getDatabaseFields().length; i++) {
                        int fieldPos = data.outputRowMeta.indexOfValue(meta.getStreamFields()[i]);
                        FieldMeta fieldMeta = null;

                        //if fieldpos =-1 then an unknown field had been added to the list
                        if (fieldPos == -1) {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInStream") + meta.getStreamFields()[i]);
                            break;
                        }

                        //search for the specified table fields in the database metadata
                        for (int ii = 1; ii <= tableDefinition.getColumnCount(); ii++) {
                            if (meta.getDatabaseFields()[i].equals(tableDefinition.getColumnName(ii))) {

                                //create fieldmeta to hold all field information needed for the bulkload
                                fieldMeta = new FieldMeta(index, tableDefinition.getColumnName(ii), tableDefinition.getColumnType(ii), tableDefinition.getPrecision(ii), tableDefinition.getScale(ii),ii);

                                //check if database field has been added before
                                for(FieldMeta fieldMetaCheck : fieldMapping.values()){
                                    if(fieldMetaCheck.getDataBasePosition().equals(ii)){
                                        stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.DuplicateDatabaseField") + fieldMetaCheck.getFieldName());
                                        break;
                                    }
                                }

                                //add streamfieldlocation and fieldmata to linkedHashMap
                                fieldMapping.put(fieldPos, fieldMeta);

                            }
                        }

                        //if value type is date or timestamp add dateformat information to the fieldMeta
                        if (inputMeta.get(fieldPos) instanceof ValueMetaDate) {
                            ValueMetaDate valueMetaDate = (ValueMetaDate) inputMeta.get(fieldPos);
                            fieldMapping.get(fieldPos).setDateFormat(valueMetaDate.getDateFormat());
                            fieldMapping.get(fieldPos).setDateTimeFormatter(DateTimeFormatter.ofPattern(valueMetaDate.getDateFormat().toPattern()));

                        }

                        index++;

                        //if the new fieldmeta field is null then no matching database field could be found
                        if (fieldMeta == null) {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + meta.getDatabaseFields()[i]);
                            break;
                        }
                    }
                } else {
                    //if fields are not specified do mapping
                    for (int i = 0; i < data.outputRowMeta.size(); i++) {
                        ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
                        FieldMeta fieldMeta = null;

                        for (int ii = 1; ii <= tableDefinition.getColumnCount(); ii++) {
                            if (valueMeta.getName().equals(tableDefinition.getColumnName(ii))) {
                                fieldMeta = new FieldMeta(i + 1, tableDefinition.getColumnName(ii), tableDefinition.getColumnType(ii), tableDefinition.getPrecision(ii), tableDefinition.getScale(ii),ii);
                                fieldMapping.put(i, fieldMeta);

                            }
                        }

                        if (inputMeta.get(i) instanceof ValueMetaDate) {
                            ValueMetaDate valueMetaDate = (ValueMetaDate) inputMeta.get(i);
                            fieldMapping.get(i).setDateFormat(valueMetaDate.getDateFormat());
                            fieldMapping.get(i).setDateTimeFormatter(DateTimeFormatter.ofPattern(valueMetaDate.getDateFormat().toPattern()));

                        }

                        if (fieldMeta == null) {
                            stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.FieldNotFoundInTable") + valueMeta.getName());
                            break;
                        }
                    }
                }


            } catch (Exception e) {
                stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.GeneralError") + e.getMessage());
            }

        }

        if (r == null) {
            // no more input to be expected...
            executeBatch();
            setOutputDone();
            try {
                connection.close();
            } catch (SQLException e) {
                stopStep(BaseMessages.getString(PKG, "MssqlBulkLoader.GeneralError") + e.getMessage());
            }
            return false;
        } else {

            //if batch length is reached push it to the database

            if (nbRowsBatch == batchSize) {
                executeBatch();
                nbRowsBatch = 0;
            }


            int index = 0;

            for (int fieldPos : fieldMapping.keySet()) {
                String value;

                //convert binary to normal
                if(inputMeta.get(fieldPos).getStorageType() == ValueMetaInterface.STORAGE_TYPE_BINARY_STRING){
                    ValueMetaInterface fromMeta = data.outputRowMeta.getValueMeta(fieldPos);
                    r[fieldPos] =fromMeta.convertBinaryStringToNativeType((byte[]) r[fieldPos]);
                }

                //write null value as empty string
                if (r[fieldPos] != null) {

                    //convert date to specified mapping
                    if (inputMeta.get(fieldPos) instanceof ValueMetaDate) {
                        value = convertDate(r[fieldPos], fieldMapping.get(fieldPos));

                    } else if (inputMeta.get(fieldPos) instanceof ValueMetaBoolean) {
                        //convert boolean to bit for MsSQL
                        Boolean bool = (Boolean) r[fieldPos];
                        value = bool.booleanValue() ? "1" : "0";

                    }
                    else {
                        value = r[fieldPos].toString();

                    }


                } else {
                    value = "";

                }


                if (index == 0) {
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


    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MssqlBulkLoaderMeta) smi;
        data = (MssqlBulkLoaderData) sdi;


        if (Utils.isEmpty(getInputRowSets())) {
            logError(BaseMessages.getString(PKG, "MssqlBulkLoader.ErrorNoInputRows"));
            return false;
        }

        return super.init(smi, sdi);
    }


    private boolean executeBatch() {
        //create inputstream to process data and reset Stringbuilder
        InputStream inputStream = new ByteArrayInputStream(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        stringBuilder.setLength(0);

        try {
            SQLServerBulkCSVFileRecord fileRecord;
            connection.setAutoCommit(false);

            if (firstBatch) {
                firstBatch = false;

                SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
                copyOptions.setTableLock(true);
                copyOptions.setUseInternalTransaction(false);
                copyOptions.setBatchSize(batchSize);

                bulkCopy.setBulkCopyOptions(copyOptions);
                bulkCopy.setDestinationTableName(destinationTable);

                //add inputfile metadata and table column mapping
                for (FieldMeta fieldmeta : fieldMapping.values()) {
                    if (fieldmeta.getFieldPosition() != null) {
                        bulkCopy.addColumnMapping(fieldmeta.getFieldPosition(), fieldmeta.getDataBasePosition());

                    }
                }
            }

            fileRecord = new SQLServerBulkCSVFileRecord(inputStream, StandardCharsets.UTF_8.name(), delimiter, false);

            //add inputfile metadata and table column mapping
            for (FieldMeta fieldmeta : fieldMapping.values()) {
                if (fieldmeta.getFieldPosition() != null) {
                    fileRecord.addColumnMetadata(fieldmeta.getFieldPosition(), fieldmeta.getFieldName(), fieldmeta.getFieldType(), fieldmeta.getPrecision(), fieldmeta.getScale(), fieldmeta.getDateTimeFormatter());

                }
            }

            bulkCopy.writeToServer(fileRecord);
            connection.commit();

        } catch (Exception e) {
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

    private String convertDate(Object dateValue, FieldMeta dateFieldMeta) {
        SimpleDateFormat simpleDateFormat = dateFieldMeta.getDateFormat();
        String date = simpleDateFormat.format((Date) dateValue);
        return date;

    }

    private void truncateTable() throws KettleDatabaseException {
        if(meta.isTruncate() && ( getCopy() == 0 && getUniqueStepNrAcrossSlaves() == 0 ) ){
                data.db.truncateTable(environmentSubstitute(meta.getSchemaName()),environmentSubstitute(meta.getTableName()));
        }
    }


}
