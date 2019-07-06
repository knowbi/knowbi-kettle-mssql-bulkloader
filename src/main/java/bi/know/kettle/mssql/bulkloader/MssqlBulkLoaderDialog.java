package bi.know.kettle.mssql.bulkloader;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class MssqlBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = MssqlBulkLoaderDialog.class; // for i18n purposes, needed by Translator2!!


    private CCombo wConnection;

    //fieldNames
    private String[] fieldNames;

    //Database fieldnames
    private String[] databaseFieldNames;

    private Label wlSchema;
    private TextVar wSchema;
    private FormData fdlSchema, fdSchema;

    private Label wlTable;
    private TextVar wTable;
    private FormData fdlTable, fdTable;

    private Label wlBatchSize;
    private TextVar wBatchSize;
    private FormData fdlBatchSize, fdBatchSize;

    private Label wlTruncate;
    private Button wTruncate;
    private FormData fdlTruncate, fdTruncate;

    private Label wlSpecifyFields;
    private Button wSpecifyFields;
    private FormData fdlSpecifyFields, fdSpecifyFields;

    private TableView wFields;
    private FormData fdFields;
    private ColumnInfo[] colHeader;


    private MssqlBulkLoaderMeta input;

    private Map<String, Integer> inputFields;

    private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();



    public MssqlBulkLoaderDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (MssqlBulkLoaderMeta) in;
        inputFields = new HashMap<String, Integer>();
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        //ModifyListener
        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
        };
        changed = input.hasChanged();

        SelectionListener lsSelection = new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
                input.setChanged();
                setTableFieldCombo();
                //validateSelection();
            }
        };

        ModifyListener lsTableMod = new ModifyListener() {
            public void modifyText( ModifyEvent arg0 ) {
                input.setChanged();
                setTableFieldCombo();
            }
        };

        SelectionAdapter lsSelMod = new SelectionAdapter() {
            public void widgetSelected( SelectionEvent arg0 ) {
                input.setChanged();
            }
        };



        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "MssqlBulkLoader.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        //add buttons
        //OK button
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$

        //ok listener
        lsOK = new Listener() {
            public void handleEvent(Event e) {
                ok();
            }
        };
        wOK.addListener(SWT.Selection, lsOK);

        //cancel button
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

        //cancel listener
        lsCancel = new Listener() {
            public void handleEvent(Event e) {
                cancel();
            }
        };
        wCancel.addListener(SWT.Selection, lsCancel);

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, null);

        //stepName label
        //create label
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG,"MssqlBulkLoader.Stepname.Label"));
        props.setLook(wlStepname);
        //create form location
        fdlStepname=new FormData();
        fdlStepname.left = new FormAttachment(0,0);
        fdlStepname.right = new FormAttachment(middle,-margin);
        fdlStepname.top = new FormAttachment(0,margin);
        //Attach to form
        wlStepname.setLayoutData(fdlStepname);

        //stepName Textbox
        //create textbox
        wStepname = new Text(shell,SWT.SINGLE|SWT.LEFT|SWT.BORDER);
        wStepname.setText(stepname);
        props.setLook(wStepname);
        //add listener
        wStepname.addModifyListener(lsMod);
        //create form location
        fdStepname=new FormData();
        fdStepname.left = new FormAttachment(middle, 0);
        fdStepname.top  = new FormAttachment(0, margin);
        fdStepname.right= new FormAttachment(100, 0);
        //attach to form
        wStepname.setLayoutData(fdStepname);

        //connection
        wConnection = addConnectionLine( shell, wStepname, middle, margin );
        if ( input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
            wConnection.select( 0 );
        }
        wConnection.addModifyListener( lsMod );
        wConnection.addModifyListener( new ModifyListener() {
            public void modifyText( ModifyEvent event ) {
                //setFlags();
            }
        } );
        wConnection.addSelectionListener( lsSelection );


        //schema

        wlSchema = new Label( shell, SWT.RIGHT );
        wlSchema.setText( BaseMessages.getString( PKG, "MssqlBulkLoader.Schema.Label" ) );
        props.setLook( wlSchema );
        fdlSchema = new FormData();
        fdlSchema.left = new FormAttachment( 0, 0 );
        fdlSchema.right = new FormAttachment( middle, -margin );
        fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
        wlSchema.setLayoutData( fdlSchema );

        wSchema = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wSchema );
        wSchema.addModifyListener( lsTableMod );
        fdSchema = new FormData();
        fdSchema.left = new FormAttachment( middle, 0 );
        fdSchema.top = new FormAttachment( wConnection, margin * 2 );
        fdSchema.right = new FormAttachment( 100, 0 );
        wSchema.setLayoutData( fdSchema );


        //table

        wlTable = new Label( shell, SWT.RIGHT );
        wlTable.setText( BaseMessages.getString( PKG, "MssqlBulkLoader.Table.Label" ) );
        props.setLook( wlTable );
        fdlTable = new FormData();
        fdlTable.left = new FormAttachment( 0, 0 );
        fdlTable.right = new FormAttachment( middle, -margin );
        fdlTable.top = new FormAttachment( wSchema, margin * 2 );
        wlTable.setLayoutData( fdlTable );

        wTable = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wSchema );
        wTable.addModifyListener( lsTableMod );
        fdTable = new FormData();
        fdTable.left = new FormAttachment( middle, 0 );
        fdTable.top = new FormAttachment( wSchema, margin * 2 );
        fdTable.right = new FormAttachment( 100, 0 );
        wTable.setLayoutData( fdTable );

        //batch size

        wlBatchSize = new Label( shell, SWT.RIGHT );
        wlBatchSize.setText( BaseMessages.getString( PKG, "MssqlBulkLoader.BatchSize.Label" ) );
        props.setLook( wlBatchSize );
        fdlBatchSize = new FormData();
        fdlBatchSize.left = new FormAttachment( 0, 0 );
        fdlBatchSize.right = new FormAttachment( middle, -margin );
        fdlBatchSize.top = new FormAttachment( wTable, margin * 2 );
        wlBatchSize.setLayoutData( fdlBatchSize );

        wBatchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wBatchSize );
        fdBatchSize = new FormData();
        fdBatchSize.left = new FormAttachment( middle, 0 );
        fdBatchSize.top = new FormAttachment( wTable, margin * 2 );
        fdBatchSize.right = new FormAttachment( 100, 0 );
        wBatchSize.setLayoutData( fdBatchSize );

        //truncate
        wlTruncate = new Label( shell, SWT.RIGHT );
        wlTruncate.setText( BaseMessages.getString( PKG, "MssqlBulkLoader.Truncate.Label" ) );
        props.setLook( wlTruncate );
        fdlTruncate = new FormData();
        fdlTruncate.left = new FormAttachment( 0, 0 );
        fdlTruncate.right = new FormAttachment( middle, -margin );
        fdlTruncate.top = new FormAttachment( wBatchSize, margin * 2 );
        wlTruncate.setLayoutData( fdlTruncate );

        wTruncate = new Button(shell, SWT.CHECK);
        props.setLook( wTruncate );
        fdTruncate = new FormData();
        fdTruncate.left = new FormAttachment( middle, 0 );
        fdTruncate.top = new FormAttachment( wBatchSize, margin * 2 );
        fdTruncate.right = new FormAttachment( 100, 0 );
        wTruncate.setLayoutData( fdTruncate );
        wTruncate.addSelectionListener( lsSelMod );

        //DatabaseFields
        wlSpecifyFields = new Label( shell, SWT.RIGHT );
        wlSpecifyFields.setText( BaseMessages.getString( PKG, "MssqlBulkLoader.SpecifyFields.Label" ) );
        props.setLook( wlSpecifyFields );
        fdlSpecifyFields = new FormData();
        fdlSpecifyFields.left = new FormAttachment( 0, 0 );
        fdlSpecifyFields.right = new FormAttachment( middle, -margin );
        fdlSpecifyFields.top = new FormAttachment( wTruncate, margin * 2 );
        wlSpecifyFields.setLayoutData( fdlSpecifyFields );

        wSpecifyFields = new Button(shell, SWT.CHECK);
        props.setLook( wSpecifyFields );
        fdSpecifyFields = new FormData();
        fdSpecifyFields.left = new FormAttachment( middle, 0 );
        fdSpecifyFields.top = new FormAttachment( wTruncate, margin * 2 );
        fdSpecifyFields.right = new FormAttachment( 100, 0 );
        wSpecifyFields.setLayoutData( fdSpecifyFields );
        wSpecifyFields.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent arg0 ) {
                wFields.setEnabled(wSpecifyFields.getSelection());
            }
        });


        //get transformation fieldnames
        //Get Fieldnames
        RowMetaInterface prevFields = null;
        try {
            prevFields = transMeta.getPrevStepFields( stepname );
            fieldNames = prevFields.getFieldNames();
        } catch (KettleStepException e) {
            logError( BaseMessages.getString( PKG, "MssqlBulkLoader.ErrorGettingFields" ) );
            fieldNames = new String[] {};
        }

        //get table fieldnames
        //TODO: get fieldnames from database meta


        int fieldsTableCols = 2;
        int fieldsTableRows = 1;

        colHeader = new ColumnInfo[fieldsTableCols];
        colHeader[0]= new ColumnInfo(BaseMessages.getString( PKG, "MssqlBulkLoader.Fields.DatabaseFieldsLabel"), ColumnInfo.COLUMN_TYPE_CCOMBO );
        colHeader[1]= new ColumnInfo(BaseMessages.getString( PKG, "MssqlBulkLoader.Fields.TransformationFieldsLabel"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames );
        tableFieldColumns.add(colHeader[0]);
        wFields = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colHeader, fieldsTableRows, false, lsMod, props);

        fdFields = new FormData();
        fdFields.left = new FormAttachment(middle,margin);
        fdFields.right = new FormAttachment( 100, 0 );
        fdFields.top = new FormAttachment(wSpecifyFields,margin);
        fdFields.bottom = new FormAttachment( wOK, -4 * margin );
        wFields.setLayoutData( fdFields );


        getData();
        setTableFieldCombo();

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return stepname;
    }


    private void getData(){
        if ( input.getDatabaseMeta() != null ) {
            wConnection.setText( input.getDatabaseMeta().getName() );
        }

        if ( input.getSchemaName() != null ) {
            wSchema.setText( input.getSchemaName());
        }

        if ( input.getTableName() != null ) {
            wTable.setText( input.getTableName());
        }

        if (input.getBatchSize() != null){
            wBatchSize.setText(input.getBatchSize());
        }
        wTruncate.setSelection(input.isTruncate());
        wSpecifyFields.setSelection(input.isSpecifyDatabaseFields());
        wFields.setEnabled(input.isSpecifyDatabaseFields());

        Table fieldsTable = wFields.table;
        fieldsTable.removeAll();

        for ( int i = 0; i < input.getDatabaseFields().length; i++ ) {
            TableItem item = new TableItem(fieldsTable,SWT.NONE);
            if ( input.getDatabaseFields()[i] != null ) {
                item.setText( 1, input.getDatabaseFields()[i] );
            }
            if ( input.getStreamFields()[i] != null ) {
                item.setText( 2, input.getStreamFields()[i] );
            }
        }


    }

    private void ok() {
        stepname = wStepname.getText();
        input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
        input.setSchemaName(wSchema.getText());
        input.setTableName(wTable.getText());
        input.setTruncate(wTruncate.getSelection());
        input.setSpecifyDatabaseFields(wSpecifyFields.getSelection());

        int nbFields = wFields.nrNonEmpty();

        input.allocate(nbFields);

        String[] databaseFields = new String[nbFields];
        String[] streamFields = new String[nbFields];

        for(int i=0;i<nbFields;i++){
            TableItem item = wFields.getNonEmpty(i);
            databaseFields[i] = Const.NVL(item.getText(1),"");
            streamFields[i] = Const.NVL(item.getText(2),"");
        }
        input.setDatabaseFields(databaseFields);
        input.setStreamFields(streamFields);


        try{
            if(!Utils.isEmpty(wBatchSize.getText())){
                Integer.parseInt(wBatchSize.getText());
                input.setBatchSize(wBatchSize.getText());
            }
        }catch (Exception e){
            MessageBox messageBox = new MessageBox(shell, SWT.ICON_ERROR);
            messageBox.setText(BaseMessages.getString(PKG, "MssqlBulkLoader.NonIntegerValue.Title"));
            messageBox.setMessage(BaseMessages.getString(PKG, "MssqlBulkLoader.NonIntegerValue.Message"));
            messageBox.open();
            return;
        }

        dispose();
    }

    //cancel function
    private void cancel() {
        stepname = null;
        input.setChanged(changed);
        dispose();
    }

    private void setTableFieldCombo() {
        Runnable fieldLoader = new Runnable() {
            public void run() {
                if ( !wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed() ) {
                    final String tableName = wTable.getText(), connectionName = wConnection.getText(), schemaName =
                            wSchema.getText();

                    // clear
                    for ( ColumnInfo colInfo : tableFieldColumns ) {
                        colInfo.setComboValues( new String[] {} );
                    }
                    if ( !Utils.isEmpty( tableName ) ) {
                        DatabaseMeta ci = transMeta.findDatabase( connectionName );
                        if ( ci != null ) {
                            Database db = new Database( loggingObject, ci );
                            try {
                                db.connect();

                                RowMetaInterface r =
                                        db.getTableFieldsMeta(
                                                transMeta.environmentSubstitute( schemaName ),
                                                transMeta.environmentSubstitute( tableName ) );
                                if ( null != r ) {
                                    String[] fieldNames = r.getFieldNames();
                                    if ( null != fieldNames ) {
                                        for ( ColumnInfo colInfo : tableFieldColumns ) {
                                            colInfo.setComboValues( fieldNames );
                                        }
                                    }
                                }
                            } catch ( Exception e ) {
                                for ( ColumnInfo colInfo : tableFieldColumns ) {
                                    colInfo.setComboValues( new String[] {} );
                                }
                                // ignore any errors here. drop downs will not be
                                // filled, but no problem for the user
                            } finally {
                                try {
                                    if ( db != null ) {
                                        db.disconnect();
                                    }
                                } catch ( Exception ignored ) {
                                    // ignore any errors here. Nothing we can do if
                                    // connection fails to close properly
                                    db = null;
                                }
                            }
                        }
                    }
                }
            }
        };
        shell.getDisplay().asyncExec( fieldLoader );
    }
}
