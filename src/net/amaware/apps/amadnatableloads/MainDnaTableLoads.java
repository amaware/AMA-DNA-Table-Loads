package net.amaware.apps.amadnatableloads;
//import net.amaware.serv.SourceServProperty;
import net.amaware.app.MainAppDataStore;
import net.amaware.autil.*;

//
/**
 * @author AMAWARE - Angelo M Adduci
 * 
 */

public class MainDnaTableLoads {
	// set Properties file key names to being used
	//Properties file 
    final static String propFileName   = "MainDnaTableLoads.properties";
	//Architecture Common communication Class 
	static ACommDb acomm;
	//Architecture Framework Class
	static MainAppDataStore mainApp;
	//Application Classes
	//static ExcelProcess _fileProcess = new ExcelProcess();
	//static PTableCodes _fileProcess = new PTableCodes();
    //	
	//
        //
		public static void main(String[] args) {
			final String thisClassName = "MainDnaTableLoads";
			//
			try { 
				acomm = new ACommDb(propFileName, args);
				
				mainApp = new MainAppDataStore(acomm, new PTableCodes(acomm,args), args, acomm.getFileTextDelimTab());
				mainApp.setSourceHeadRowStart(1);
				mainApp.setSourceDataHeadRowStart(3);
				//mainApp.setSourceDataHeadRowEnd(1);
				mainApp.setSourceDataRowStart(4);
				//mainApp.setSourceDataRowEnd(10);
				
				mainApp.doProcess(acomm, thisClassName);
				
				//mainApp.getHtmlServ().outPageLine(acomm, thisClassName+" completed ");
				acomm.end();
				
			} catch (AException e1) {
				throw e1;
			}
			
		}
//
// END CLASS
//	
}
