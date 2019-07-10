package bi.know.kettle.mssql.bulkloader;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

public class FieldMeta {
    private String fieldName;
    private Integer fieldType;
    private Integer precision;
    private Integer scale;
    private Integer fieldPosition;
    private SimpleDateFormat dateFormat=null;
    private DateTimeFormatter dateTimeFormatter = null;



    public FieldMeta(Integer fieldPosition,String fieldName,Integer fieldType,Integer precision, Integer scale){
        this.fieldPosition = fieldPosition;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.precision  = precision;
        this.scale = scale;
    }

    @Override
    public String toString() {
        String message = "Fieldname: "+this.fieldName + "\n";
        message += "FieldType: "+this.fieldType + "\n";
        message += "Precision: "+this.precision + "\n";
        message += "Scale: "+this.scale + "\n";
        return message;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getFieldType() {
        return fieldType;
    }

    public void setFieldType(Integer fieldType) {
        this.fieldType = fieldType;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Integer getFieldPosition() {
        return fieldPosition;
    }

    public void setFieldPosition(Integer fieldPosition) {
        this.fieldPosition = fieldPosition;
    }

    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }

    public void setDateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    public SimpleDateFormat getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(SimpleDateFormat dateFormat) {
        this.dateFormat = dateFormat;
    }
}
