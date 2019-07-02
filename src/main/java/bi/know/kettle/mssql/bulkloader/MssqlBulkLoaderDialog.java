package bi.know.kettle.mssql.bulkloader;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import java.util.HashMap;
import java.util.Map;

public class MssqlBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = MssqlBulkLoaderDialog.class; // for i18n purposes, needed by Translator2!!


    private CCombo wConnection;

    private Label wlSchema;
    private TextVar wSchema;
    private FormData fdlSchema, fdSchema;

    private Label wlTable;
    private TextVar wTable;
    private FormData fdlTable, fdTable;

    private Label wlBatchSize;
    private TextVar wBatchSize;
    private FormData fdlBatchSize, fdBatchSize;


    private MssqlBulkLoaderMeta input;

    private Map<String, Integer> inputFields;



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
                //setTableFieldCombo();
                //validateSelection();
            }
        };

        ModifyListener lsTableMod = new ModifyListener() {
            public void modifyText( ModifyEvent arg0 ) {
                input.setChanged();
                //setTableFieldCombo();
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
        wBatchSize.addModifyListener( lsTableMod );
        fdBatchSize = new FormData();
        fdBatchSize.left = new FormAttachment( middle, 0 );
        fdBatchSize.top = new FormAttachment( wTable, margin * 2 );
        fdBatchSize.right = new FormAttachment( 100, 0 );
        wBatchSize.setLayoutData( fdBatchSize );

        //truncate



        getData();
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


    }

    private void ok() {
        stepname = wStepname.getText();
        input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
        input.setSchemaName(wSchema.getText());
        input.setTableName(wTable.getText());

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
}
