/**
 * 
 */
package net.amaware.apps.amadnatableloads;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

import com.amaware.dna.query.TABLE_CODES;

import net.amaware.app.DataStoreReport;

import net.amaware.autil.AComm;
import net.amaware.autil.ACommDb;
import net.amaware.autil.ADataColResult;
import net.amaware.autil.ADatabaseAccess;
import net.amaware.autil.AException;
import net.amaware.autil.AExceptionSql;
import net.amaware.autil.AFileExcelPOI;
import net.amaware.autil.AProperties;

//import net.amaware.serv.DataStore;
import net.amaware.serv.HtmlTargetServ;
import net.amaware.serv.SourceProperty;
//import net.amaware.autil.ASqlStatements;
//import net.amaware.serv.SourceServProperty;



/**
 * @author PSDAA88 - Angelo M Adduci - Sep 6, 2005 3:02:12 PM
 *
 */

public class PTableCodes extends DataStoreReport {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	final String thisClassName = this.getClass().getName();
	//
	//*SqlApp AutoGen @2015-09-30 21:58:06.0
    protected ADataColResult fId = mapDataCol("id");
    protected ADataColResult fTabName = mapDataCol("tab_name");
    protected ADataColResult fCodeName = mapDataCol("code_name");
    protected ADataColResult fCodeValue = mapDataCol("code_value");
    protected ADataColResult fUserModId = mapDataCol("user_mod_id");
    protected ADataColResult fUserModTs = mapDataCol("user_mod_ts");
    protected ADataColResult fTestIntNull = mapDataCol("test_int_null");
    protected ADataColResult fTestTsNull = mapDataCol("test_ts_null");
   //
    //
	//------------------db area-----------------------------
    //
    final static String propFileDbDnaLOGS     = "DbDnaLOGS.properties";
    final static String propFileDbAmawareLOGS = "DbAmawareLOGS.properties"; 

    final static String propFileDbDnaTABLE_CODES     = "DbDnaTABLE_CODES.properties";
    final static String propFileDbAmawareTABLE_CODES = "DbAmawareTABLE_CODES.properties"; 
    //
    ADatabaseAccess thisADatabaseAccess;    
    AFileExcelPOI aFileExcelPOI;
    //    
    
    //        
   // 
   TABLE_CODES qTABLE_CODES = null;
   //
   int numRowsIn=0;
   int numRowsToProcess=0;
   int numRowsInserted=0;
   int numRowsDup=0;
   //
   String transTS="";
   String transEndDte="9999-12-31";
    //
	int displayNum=0, displayAtNum=1000;
	int displayRowCnt=0;
	//
	ACommDb acomm=null;
	String[] args=null;
	//
	/**
	 * 
	 */
	public PTableCodes(ACommDb iacomm,String[] iargs) {
		super();
		acomm=iacomm;
		args=iargs;
	}
	
	

	public DataStoreReport processThis(ACommDb acomm
			, SourceProperty _aProperty, HtmlTargetServ _aHtmlServ) {
		super.processThis(acomm, _aProperty, _aHtmlServ); // always call this first
		
		getThisHtmlServ().outPageLine(acomm,  thisClassName+"=>processThis");
		
		
		_aProperty.displayProperties(acomm);	
		
		transTS=acomm.getCurrTimestampNew();
		
		return this;
	}
	
	
	/*
	 * 
	 * 
	 */
	
    public String doRequest() { 
        return 
         "REPORT-BREAK 1"
         + "REPORT-BREAK-SUM 2"
         +"; "
         ;
       }

	public boolean doSourceStarted(ACommDb acomm) {
		int _currRowNum = getDataRowNum();		
		boolean retb = true;
	

		acomm.addPageMsgsLineOut(" ");
		
		qTABLE_CODES = new TABLE_CODES(acomm, propFileDbDnaTABLE_CODES, args);

		return retb;
	}
	/*
	 * 
	 * 
	 */
    
	public boolean doDataRowsNotFound(ACommDb acomm)
	throws AException {
		super.doDataRowsNotFound(acomm);
		
		//getThisHtmlServ().outPageLine(acomm,  "DataRowsNotFound");
		
		return true; 

	}

	/*
	 * 
	 * 
	 */
    
    
	public boolean doDataRow(ACommDb acomm, AException _exceptionSql, boolean _isRowBreak)
	throws AException {
		
		//super.doDataRow(acomm, _exceptionSql, _isRowBreak); // sends pout row
		
		//getThisHtmlServ().outPageLine(acomm,  "DataRowFound");
		++numRowsIn;
		
		if (numRowsIn < 1460) {
			//return true;
		}
		
		
		try {
			
			//getThisHtmlServ().outPageRow(acomm, this);
			
			++numRowsToProcess;			
			
		    fUserModTs.setColumnValue(transTS); //set to current ts
			//fUserModTs.setColumnValue("2017-04-15 06:45:53.00"); //set to current ts
			//fUserModTs.setColumnValue("2017-04-15 06:45:53"); //set to current ts
		    
		    fUserModId.setColumnValue(acomm.getDbUserID());
 		    //fNotes.setColumnValue(doFieldValidateString(acomm, fNotes.getColumnValue()));
 		   // 
 		  

		    fTestTsNull.setColumnValue("");
		    //fTestTsNull.setColumnValue(transTS);
		    
 		  //
 		    
			//if (fAddressZIP.getColumnValue().contains("11228")) {
				   
			doDSRFieldsValidate(acomm);
			
		    //fTestIntNull.setColumnValue(""); //here because Validate move zero as default to notnum fields
			
			doDSRFieldsToTableTABLE_CODES(acomm, qTABLE_CODES);

			getThisHtmlServ().outPageRow(acomm, this);

			//
			try {
				//qTABLE_CODES.doProcessInsertRow(acomm, qTABLE_CODES.getInsertStatement(acomm));
                String tableName="table_codes";
				qTABLE_CODES.doProcessInsertRow(qTABLE_CODES.getAcomm()
						, qTABLE_CODES.getInsertStatement(acomm,tableName
			    				,Arrays.asList(new ADataColResult(tableName,TABLE_CODES.ID,fId.getColumnValue(),false)
			    						      ,new ADataColResult(tableName,TABLE_CODES.TAB_NAME,fTabName.getColumnValue(),true)
		    						          ,new ADataColResult(tableName,TABLE_CODES.CODE_NAME,fCodeName.getColumnValue(),true)
		    						          ,new ADataColResult(tableName,TABLE_CODES.CODE_VALUE,fCodeValue.getColumnValue(),true)
		    						          
		    						          //,new ADataColResult(tableName,TABLE_CODES.USER_MOD_ID,fUserModId.getColumnValue(),true)
		    						          ,new ADataColResult(tableName,TABLE_CODES.USER_MOD_ID,acomm.getDbUserID(),true)
		    						          
		    						          
		    						      //,new ADataColResult(tableName,TABLE_CODES.ID,fId.getColumnValue(),true)
		    						      )
		    				)
						);
			
			} catch (AExceptionSql e1) {
				if (e1.isExceptionSqlRowDuplicate(acomm)) { //
					++numRowsDup;
					getThisHtmlServ().outPageLineError(acomm,
							"...DUP Row NOT Inserted for {" + qTABLE_CODES.getSqlStatementUse() + "}"
									+ "<br/><BR>msg{" + e1.getExceptionMsg() + "}");

					acomm.addPageMsgsLineOut(thisClassName + "=>NOT INSERTED Row#{" + numRowsIn + "}"
							+ " |#RowsInserted{" + numRowsInserted + "}");

				} else {
					getThisHtmlServ().outPageLine(acomm, 1, "...Row NOT Inserted for {"
							+ qTABLE_CODES.getSqlStatementUse() + "}" + "<br/> msg{" + e1.getExceptionMsg() + "}");

					throw e1;
				}
			}
			numRowsInserted += qTABLE_CODES.getPsNumRowsInserted();
			if (qTABLE_CODES.getPsNumRowsInserted() > 0) {

				getThisHtmlServ().outPageLine(acomm, 1, "...Row Inserted " // for
																			// {"+qMERCHANT_TRANS_NYC.getInsertStatement(acomm)+"}"
				);

				++displayNum;
				if (displayNum >= displayAtNum) {
					acomm.addPageMsgsLineOut(
							thisClassName + "=>Row#{" + numRowsIn + "}" + " |#RowsInserted{" + numRowsInserted + "}");
					displayNum = 0;
				}

			}
			/*	   
			if (numRowsInserted ==3) {
				acomm.addPageMsgsLineOut(thisClassName+"=>Commit @ insert #{" + numRowsInserted +  "}");
				acomm.dbConCommit();
				//throw new AException(acomm, acomm.addPageMsgsLineOut(thisClassName+"=>Abending to see ROLLBACK work @ insert #{" + numRowsInserted +  "}"));
			}
			if (numRowsInserted > 5) {
				acomm.addPageMsgsLineOut(thisClassName+"=>Abending to see ROLLBACK work @ insert #{" + numRowsInserted +  "}");
				throw new AException(acomm, acomm.addPageMsgsLineOut(thisClassName+"=>Abending to see ROLLBACK work @ insert #{" + numRowsInserted +  "}"));
			}
			*/
			
			
			if (numRowsIn == 100000) {
				acomm.addPageMsgsLineOut(thisClassName+"=>Stop Processng file #{" + numRowsIn +  "}");
				return false;
			}
			
/*			
			if (_currRowNum == 10) {
				getThisHtmlServ().outPageLineCol(acomm,
				    4, "This is Row#" +_currRowNum + " on 4th col");
				getThisHtmlServ().outPageLine(acomm,
					    1, "This is Row#" +_currRowNum + " Line at 1st col");
			   
			}

			if (_currRowNum == 13) {
				getThisHtmlServ().outPageLineCol(acomm,
					    7, "This is Row#" +_currRowNum + " on 7th col");
				   
				}
			
*/			
	    } catch (AExceptionSql e1) {
			throw new AException(acomm, e1, thisClassName
					+ "=>SQL Exception @Row#" + numRowsIn
			        + " |SQLCode{" + e1.getExceptionCode() + "}"
			        + " |SQLMsg{" + e1.getExceptionMsg() + "}"
			        );
			
		} catch (Exception e) {
			throw new AException(acomm, e, thisClassName
					+ "=>doSelectRowCurr=>Row#" + numRowsIn);
			
		}
		
		
		return true; //or false to stop processing of file

	}

	/**
	 * @return 
	 */
	public boolean doDataRowBreak(ACommDb acomm) throws AException {

		int _currRowNum = getDataRowNum();
		/*
	    aHtmlServ.outPageTableLine(acomm,
			    1, "RowBreak Pre at Row#" +_currRowNum);
				
		aHtmlServ.outRowBreak(acomm, this);

	    aHtmlServ.outPageTableLine(acomm,
			    1, "RowBreak Post at Row#" +_currRowNum);
		*/
		
		return super.doDataRowBreak(acomm);
	}
	

	public boolean doDataRowsEnded(ACommDb acomm)
	throws AException {
		
		super.doDataRowsEnded(acomm);

		//getThisHtmlServ().outPageLineCol(acomm, 4, "This is on 4th col");
		
		getThisHtmlServ().outPageLineHi(acomm, 1, "DataRows at End");
		getThisHtmlServ().outPageLine(acomm, 2, "#Rows in{"+numRowsIn+"}");
		getThisHtmlServ().outPageLine(acomm, 2, "#Rows To Process{"+numRowsToProcess+"}");
		
		getThisHtmlServ().outPageLine(acomm, 2, "#Rows Inserted{"+numRowsInserted+"}");
		getThisHtmlServ().outPageLine(acomm, 2, "#Rows were duplicate{"+numRowsDup+"}");
		
		getThisHtmlServ().writeReportTextLine("___________________________________________",  ' ');
		getThisHtmlServ().writeReportTextLine("__________________TOTALS___________________",  ' ');
		getThisHtmlServ().writeReportTextLine("#Rows in{"+numRowsIn+"}",  ' ');
		getThisHtmlServ().writeReportTextLine("#Rows To Process{"+numRowsToProcess+"}",  ' ');
		getThisHtmlServ().writeReportTextLine("#Rows Inserted{"+numRowsInserted+"}",  ' ');
		getThisHtmlServ().writeReportTextLine("#Rows were duplicate{"+numRowsDup+"}",  ' ');
		
        acomm.addPageMsgsLineOut(thisClassName+"=>TotRows{" + numRowsIn +  "}"
		    +" |#RowsInserted{"  + numRowsInserted +  "}" 
		    );

		
		return true; //or false to stop processing of file

	}

	public boolean doDataRowAsExcel(ACommDb acomm) {
		
		String outExcelFileName = acomm.getOutFileDirectoryWithClassName()+AComm.getArgFileName();
		
		aFileExcelPOI = new AFileExcelPOI(acomm, outExcelFileName);
		acomm.addPageMsgsLineOut(
			    "PTableCodes:doDataRowAsExcel=>"
			  + " |filename{" + outExcelFileName +"}"
			 );		
        //	
		
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbDnaTABLE_CODES);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "table_codes-DNA"
                                              , "Select *"
                                          +" from table_codes " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            //+ " order by tab_name"
                            + " order by tab_name, code_name, code_value"
                            );     
		
		//
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbDnaLOGS);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "logs-DNA"
                                              , "Select *"
                                          +" from logs " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            + " order by entry_type, entry_subject, entry_topic"
                            );     
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbDnaLOGS);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "logs-DNA-ByCreateTS"
                                              , "Select *"
                                          +" from logs " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            + " order by create_ts desc, entry_type, entry_subject, entry_topic"
                            );             
        //
        //		
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbAmawareTABLE_CODES);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "table_codes-Amaware"
                                              , "Select *"
                                          +" from table_codes " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            //+ " order by tab_name"
                            + " order by tab_name, code_name, code_value"
                            );     
		
		//
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbAmawareLOGS);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "logs-Amaware"
                                              , "Select *"
                                          +" from logs " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            + " order by entry_type, entry_subject, entry_topic"
                            );     
        thisADatabaseAccess = new ADatabaseAccess(acomm, propFileDbAmawareLOGS);
        thisADatabaseAccess.doQueryRsExcel(aFileExcelPOI
                                              , "logs-Amaware-ByCreateTS"
                                              , "Select *"
                                          +" from logs " 
                            //+ " Where field_nme  = '" + ufieldname +"'" 
                                          
                            + " order by create_ts desc, entry_type, entry_subject, entry_topic"
                            );     
        //
   		try {
			aFileExcelPOI.doOutputEnd();
		} catch (IOException e) {
			throw new AException(acomm, e, " Close of outFileExcel");
		}        
        //
	    
	       return true;
	}
	

	//*SqlApp AutoGen @2017-03-18 07:57:51.0
	 public void doDSRFieldsToTableTABLE_CODES(ACommDb acomm) {
	             doDSRFieldsToTableTABLE_CODES(acomm,  qTABLE_CODES);
	//
	 } //End doDSRFieldsToTableTABLE_CODES qTABLE_CODES
	//
	//
	 public void doDSRFieldsToTableTABLE_CODES(ACommDb acomm, TABLE_CODES _qClass) {

		   _qClass.setDataColResultValue(fId.getColumnTitle(),fId.getColumnValue());
		   _qClass.setDataColResultValue(fTabName.getColumnTitle(),fTabName.getColumnValue());
		   _qClass.setDataColResultValue(fCodeName.getColumnTitle(),fCodeName.getColumnValue());
		   _qClass.setDataColResultValue(fCodeValue.getColumnTitle(),fCodeValue.getColumnValue());
		   _qClass.setDataColResultValue(fUserModId.getColumnTitle(),fUserModId.getColumnValue());
		   _qClass.setDataColResultValue(fUserModTs.getColumnTitle(),fUserModTs.getColumnValue());
		   
		//
		 } //End doDSRFieldsToTable TABLE_CODES _qClass
		//
	//
	//* SqlApp DataStoreReport SET Data Fields from Table columns
	//
	//*SqlApp AutoGen @2017-03-18 07:57:51.0
	 public void doDSRFieldsFromTableTABLE_CODES(ACommDb acomm) {
	             doDSRFieldsFromTableTABLE_CODES(acomm,  qTABLE_CODES);
	//
	 } //End doDSRFieldsFromTableTABLE_CODES
	//
	 public void doDSRFieldsFromTableTABLE_CODES(ACommDb acomm, TABLE_CODES _qClass) {
		    
	        //fId.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.ID));
		    fTabName.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.TAB_NAME));
		    fCodeName.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.CODE_NAME));
		    fCodeValue.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.CODE_VALUE));
		    fUserModId.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.USER_MOD_ID));
		    fUserModTs.setColumnValue(_qClass.getDataColResultValue(TABLE_CODES.USER_MOD_TS));
		    

		//
		 } //End doDSRFieldsFromTable qTABLE_CODES
		//
	//
	//* SqlApp DataStoreReport Validate input  Data Fields
	//
	//*SqlApp AutoGen @2017-03-18 07:57:51.0
	 public void doDSRFieldsValidate(ACommDb acomm) throws Exception {
		    fId.setColumnValue(String.valueOf(doFieldValidateInt(acomm, fId.getColumnValue(),0)));
		    fTabName.setColumnValue(doFieldValidateString(acomm, fTabName.getColumnValue()));
		    fCodeName.setColumnValue(doFieldValidateString(acomm, fCodeName.getColumnValue()));
		    fCodeValue.setColumnValue(doFieldValidateString(acomm, fCodeValue.getColumnValue()));
		    fUserModId.setColumnValue(doFieldValidateString(acomm, fUserModId.getColumnValue()));
		    fUserModTs.setColumnValue(doFieldValidateString(acomm, fUserModTs.getColumnValue()));
		    fTestIntNull.setColumnValue(String.valueOf(doFieldValidateInt(acomm, fTestIntNull.getColumnValue(),0)));
		    fTestTsNull.setColumnValue(doFieldValidateString(acomm, fTestTsNull.getColumnValue()));
		//
		 } //End doDSRFieldsValidate
		//

//
//END
//	
}
