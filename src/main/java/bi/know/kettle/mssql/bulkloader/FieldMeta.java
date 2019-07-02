package bi.know.kettle.mssql.bulkloader;

public class FieldMeta {
    //TODO: add input kettle fieldname (helps for the mapping later)
    //TODO: add datetime formatter
    String fieldName;
    Integer FieldType;
    Integer Precision;
    Integer Scale;
    Integer FieldPosition;

    public FieldMeta(String fieldName,Integer FieldType,Integer Precision, Integer Scale){
        this.fieldName = fieldName;
        this.FieldType = FieldType;
        this.Precision  = Precision;
        this.Scale = Scale;
    }

    @Override
    public String toString() {
        String message = "Fieldname: "+this.fieldName + "\n";
        message += "FieldType: "+this.FieldType + "\n";
        message += "Precision: "+this.Precision + "\n";
        message += "Scale: "+this.Scale + "\n";
        return message;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getFieldType() {
        return FieldType;
    }

    public void setFieldType(Integer fieldType) {
        FieldType = fieldType;
    }

    public Integer getPrecision() {
        return Precision;
    }

    public void setPrecision(Integer precision) {
        Precision = precision;
    }

    public Integer getScale() {
        return Scale;
    }

    public void setScale(Integer scale) {
        Scale = scale;
    }

    public Integer getFieldPosition() {
        return FieldPosition;
    }

    public void setFieldPosition(Integer fieldPosition) {
        FieldPosition = fieldPosition;
    }

}
