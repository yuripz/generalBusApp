package net.plumbing.msgbus.threads.utils;
//import oracle.jdbc.OracleCallableStatement;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplateVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
//import oracle.jdbc.OracleResultSetMetaData;
//import oracle.jdbc.OracleTypes;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
//import java.util.Arrays;
//import org.postgresql.jdbc.PgResultSet;
import org.slf4j.Logger;
//import DataAccess;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.init.ConfigMsgTemplates;

//import javax.validation.constraints.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.List;

import com.github.underscore.U;

// import static net.plumbing.msgbus.common.ClientIpHelper.toCamelCase;
//import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;


public class CustomJavaMethods {
	private static final String  TagNameHead       = "SQLRequest";
	private static final String  TagNameSQLStatement  = "SQLStatement";
	private static final String  TagNameSubSelectStatement  = "SubSelectStatement";
	private static final String  AttrNameStateType  = "type";
	private static final String  AttrNameStateNum   = "snum";
	private static final String  TagNamePSTMT      = "PSTMT";
	private static final String  TagNameParam      = "Param";
	private static final String  AttrNameParamNum  = "pnum";
	private static final String  OperTypeSel     = "select";
	private static final String  RowTag = "Record";

	public static int GetDBConfigEntry(MessageQueueVO messageQueueVO, MessageDetails messageDetails,
									     Logger Message_Log)  {
		boolean isDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();
		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Queue_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String Queue_Id_Value;
		if (elmtTemplate_Id != null) { Queue_Id_Value= elmtTemplate_Id.getText(); }
		else Queue_Id_Value="";
		//long Queue_Id= Long.parseLong(Queue_Id_Value);
		String DataAccessProperties;
//		try {
		if ( Queue_Id_Value.equalsIgnoreCase("0123456789")) {
			DataAccessProperties =  "HrmsPoint='" + ApplicationProperties.HrmsPoint + "'\n"
					+ "hrmsDbLogin='" + ApplicationProperties.hrmsDbLogin + "'\n"
					+ "hrmsDbPasswd='" + ApplicationProperties.hrmsDbPasswd + "'\n"
					+ "ExtSysPoint='" + ApplicationProperties.ExtSysPoint + "'\n"
					+ "ExtSysDbLogin='" + ApplicationProperties.ExtSysDbLogin + "'\n"
					+ "ExtSysDbPasswd='" + ApplicationProperties.ExtSysDbPasswd + "'\n"
					+ "pSQLFunctionRun" + ApplicationProperties.pSQLFunctionRun + "'\n";
		}
		else {
			DataAccessProperties = "Queue_Id=" + Queue_Id_Value + "'\n"
					+ "HrmsPoint='" + ApplicationProperties.HrmsPoint + "'\n"
					+ "ExtSysPoint='" + ApplicationProperties.ExtSysPoint + "'\n";
		}
		String encodedAuth = "Queue_Id='" + Queue_Id_Value + "'\nAF"
							+ Base64.getEncoder()
							.encodeToString((DataAccessProperties).getBytes(StandardCharsets.UTF_8));

			if ( isDebugged ) {
				Message_Log.info("[" + messageQueueVO.getQueue_Id() + "] Hermes_Connection.getClientInfo() :`" + DataAccessProperties + "`");
			}
			messageDetails.XML_MsgResponse.append(encodedAuth);
//		}
//		catch (SQLException e) {
//			messageDetails.XML_MsgResponse.setLength(0); messageDetails.XML_MsgResponse.trimToSize();
//			messageDetails.XML_MsgResponse.append(XMLchars.Fault_Server_noNS_Begin).append(e.getMessage()).append(XMLchars.Fault_noNS_End);
//			e.printStackTrace();
//		}
//		messageDetails.XML_MsgResponse.setLength(0);
//		messageDetails.XML_MsgResponse.append(XMLchars.nanXSLT_Result);
		return 0;
	}

	public static int GetRequest_Body4Message(MessageQueueVO messageQueueVO, MessageDetails messageDetails,
                                              TheadDataAccess theadDataAccess, Logger MessegeReceive_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		messageDetails.XML_MsgResponse.setLength(0);
		XPathExpression<Element> xpathQueue_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Queue_Id", Filters.element());
		Element elmtQueue_Id = xpathQueue_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtQueue_Id== null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе GetRequest_Body4Message не найден параметр Parametrs/QueryString/Queue_Id");
			return -33;
		}
		String Queue_Id_Value= elmtQueue_Id.getText();
		long Queue_Id= Long.parseLong(Queue_Id_Value);
		/*messageDetails.XML_MsgResponse.append( "<?xml version=\"1.0\" encoding=\"UTF-8\"?><SaveGeoObjectResponse>\n" +
				"\t<ResponseCode>10</ResponseCode><ResponseMessage>106891449</ResponseMessage>\n" +
				"</SaveGeoObjectResponse>" );
		*/
		boolean IsDebugged = false; //true;
		int nn = MessageUtils.ReadMessage( theadDataAccess,  Queue_Id,  messageDetails, IsDebugged, MessegeReceive_Log);
		if (nn >= 0) {
			// -- без formatXml - в одну строку, некрасиво на Форнте

			if ((messageDetails.XML_MsgResponse.length() > XMLchars.nanXSLT_Result.length())) {
				// копируем XML_MsgResponse в XML_MsgClear для форматирования
						messageDetails.XML_MsgClear.setLength(0);
						messageDetails.XML_MsgClear.append(messageDetails.XML_MsgResponse);
				try {
					// форматируем из копии XML_MsgClear в XML_MsgResponse для отправки

					messageDetails.XML_MsgResponse.setLength(0);
					messageDetails.XML_MsgResponse.append(U.formatXml(messageDetails.XML_MsgClear.toString()));
				} catch (Exception e) {
					// неотфармотировался XML, отдаём обратно как есть
					messageDetails.XML_MsgResponse.append(messageDetails.XML_MsgClear);
				}
			}
			else
				messageDetails.XML_MsgResponse.append(XMLchars.nanXSLT_Result);

			return 0;
		}
		else {
			messageDetails.XML_MsgResponse.setLength(0);
			messageDetails.XML_MsgResponse.append(XMLchars.nanXSLT_Result);
			return nn;
		}

	}

	public static int GetRequest_Confirmation4Message( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
											   TheadDataAccess theadDataAccess,Logger MessegeReceive_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Queue_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String Queue_Id_Value= elmtTemplate_Id.getText();
		long Queue_Id= Long.parseLong(Queue_Id_Value);

		int nn = MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, messageDetails, MessegeReceive_Log);
		messageDetails.XML_MsgResponse.setLength(0);
		// -- без formatXml - в одну строку, некрасиво на Форнте
		if ( (nn > 0) && (messageDetails.XML_MsgConfirmation.length() > XMLchars.nanXSLT_Result.length() ))
			try {
				messageDetails.XML_MsgResponse.append(U.formatXml(messageDetails.XML_MsgConfirmation.toString()));
			} catch ( Exception e) {
				// неотфармотировался XML, отдаём обратно как есть
				messageDetails.XML_MsgResponse.append(messageDetails.XML_MsgConfirmation);
			}
		else
			messageDetails.XML_MsgResponse.append(XMLchars.nanXSLT_Result);

		if (nn >= 0) {
			return 0;
		}
		else
			return nn;
	}

	public static int GetRequest4MessageQueueLog( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
													   TheadDataAccess theadDataAccess, String dbSchema, Logger MessegeReceive_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		boolean isDebugged=true;

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_RowId", Filters.element());
		Element elmtRow_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String QueueLog_RowId_base64Value= elmtRow_Id.getText();
		//byte[] decoded = Base64.getDecoder().decode(QueueLog_RowId_base64Value);
		//String decodedStr = new String(decoded, StandardCharsets.UTF_8);
		String QueueLog_RowId_Value = new String(Base64.getDecoder().decode(QueueLog_RowId_base64Value), StandardCharsets.UTF_8);

		// RowId ROWID_QUEUElog= RowId...parseLong(QueueLog_RowId_Value);
		String SELECT_QUEUElog_Response;
		if (! theadDataAccess.rdbmsVendor.equals("oracle"))
			SELECT_QUEUElog_Response ="select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID = ?";
			else
			SELECT_QUEUElog_Response ="select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID = CHARTOROWID(?)";

		PreparedStatement stmt_SELECT_QUEUElog_Response = null;
		messageDetails.XML_MsgClear.setLength(0);
		try {
			stmt_SELECT_QUEUElog_Response = theadDataAccess.Hermes_Connection.prepareStatement( SELECT_QUEUElog_Response );
			stmt_SELECT_QUEUElog_Response.setString(1, QueueLog_RowId_Value);

			ResultSet rs = stmt_SELECT_QUEUElog_Response.executeQuery();
			while (rs.next()) {
				messageDetails.XML_MsgClear.append( rs.getString("Request"));
				//MessageQueueLogResponse = rs.getString("Request");
				if ( isDebugged )
					MessegeReceive_Log.info( "["+ messageQueueVO.getQueue_Id() +"] select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "'");
			}
			rs.close();
			stmt_SELECT_QUEUElog_Response.close();
			stmt_SELECT_QUEUElog_Response = null;

		}catch (SQLException e) {
			MessegeReceive_Log.info( "["+ messageQueueVO.getQueue_Id() +"] select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "'; fault {}",
					e.getMessage() );

			e.printStackTrace();
			if ( stmt_SELECT_QUEUElog_Response != null )
				try { stmt_SELECT_QUEUElog_Response.close(); } catch (SQLException ex) { ex.printStackTrace(); }
			return -1;
		}
		messageDetails.XML_MsgResponse.setLength(0);

		if (  (messageDetails.XML_MsgClear.length() > XMLchars.nanXSLT_Result.length() ))
			try {
				messageDetails.XML_MsgResponse.append(U.formatXml(messageDetails.XML_MsgClear.toString()));
			} catch ( Exception e) {
				// неотфармотировался XML, отдаём обратно как есть
				messageDetails.XML_MsgResponse.append(messageDetails.XML_MsgClear);
			}
		else
			messageDetails.XML_MsgResponse.append(XMLchars.nanXSLT_Result);

		return 0;
	}

	public static int GetResponse4MessageQueueLog( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
												  TheadDataAccess theadDataAccess, String dbSchema, Logger MessegeReceive_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		boolean isDebugged=true;

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_RowId", Filters.element());
		Element elmtRow_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме

		String QueueLog_RowId_base64Value= elmtRow_Id.getText();
		//byte[] decoded = Base64.getDecoder().decode(QueueLog_RowId_base64Value);
		//String decodedStr = new String(decoded, StandardCharsets.UTF_8);
		String QueueLog_RowId_Value = new String(Base64.getDecoder().decode(QueueLog_RowId_base64Value), StandardCharsets.UTF_8);
		// RowId ROWID_QUEUElog= RowId...parseLong(QueueLog_RowId_Value);
		String SELECT_QUEUElog_Response="select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID = ?";

		// PreparedStatement stmt_SELECT_QUEUElog_Response;
		//String MessageQueueLogResponse = null;
		PreparedStatement stmt_SELECT_QUEUElog_Response = null;
		messageDetails.XML_MsgResponse.setLength(0);
		try {
			stmt_SELECT_QUEUElog_Response = theadDataAccess.Hermes_Connection.prepareStatement( SELECT_QUEUElog_Response );
			stmt_SELECT_QUEUElog_Response.setString(1, QueueLog_RowId_Value);
			ResultSet rs = stmt_SELECT_QUEUElog_Response.executeQuery();
			while (rs.next()) {
				messageDetails.XML_MsgResponse.append(rs.getString("Response"));
				//MessageQueueLogResponse = rs.getString("Response");
				if ( isDebugged )
					MessegeReceive_Log.info( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "'");

			}
			rs.close();
			stmt_SELECT_QUEUElog_Response.close();
			stmt_SELECT_QUEUElog_Response = null;

		}catch (SQLException e) {
			MessegeReceive_Log.error( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage());
			System.err.println( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage());
			messageDetails.XML_MsgResponse.append("["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage() );
			e.printStackTrace();
			if ( stmt_SELECT_QUEUElog_Response != null )
				try { stmt_SELECT_QUEUElog_Response.close(); } catch (SQLException ex) { ex.printStackTrace(); }
			return -1;
		}

			return 0;
	}

	public static int ReplaceConfirmation_4_MessageQueue ( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
												   TheadDataAccess theadDataAccess,  Logger MessegeReceive_Log) {
		XPathExpression<Element> xpathQueue_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/QueryString/x_Queue_Id", Filters.element());
		Element elmtQueue_Id = xpathQueue_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtQueue_Id== null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе ReplaceConfirmation4MessageQueue не найден параметр Parametrs/QueryString/Queue_Id");
			return -33;
		}
		String Pk_Value= elmtQueue_Id.getText();
		//String ParamElements[] = Pk_Value.split("-@-");
		long Queue_Id_4_ReplaceConfirmation = Long.parseLong(Pk_Value);

		XPathExpression<Element> xpathConfirmationContent_4_Save = XPathFactory.instance().compile("/Envelope/Body/Parametrs/Data", Filters.element());
		Element elmtConfirmationContent_4_Save = xpathConfirmationContent_4_Save.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtConfirmationContent_4_Save == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе ReplaceConfirmation4MessageQueue не найден параметр Parametrs/Data");
			return -36;
		}

		String confirmationContent_4_Replace = elmtConfirmationContent_4_Save.getText();
		int Function_Result;
		try {
			Function_Result = PerformSaveRequest.ReplaceConfirmation_4_MessageQueue(
					theadDataAccess, Queue_Id_4_ReplaceConfirmation, messageDetails, confirmationContent_4_Replace, MessegeReceive_Log);
		}
		catch (Exception e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(" В системе Java метод:`ReplaceConfirmation4MessageQueue сбой при вызове SaveRequestBody_4_MessageQueue=(" + confirmationContent_4_Replace + ") fault" + e.getMessage() );
			MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "] В системе Java метод:`ReplaceConfirmation4MessageQueue` сбой при вызове SaveRequestBody_4_MessageQueue=(" + confirmationContent_4_Replace + ") fault" + e.getMessage() );
			Function_Result = -1;

		}
		if ( Function_Result != 0) {
			//messageDetails.MsgReason.append(" В системе Java метод:`ReplaceConfirmation4MessageQueue");
			messageDetails.MsgReason.append("` , проконсультируйтесь с разработчиками");
		}
		else {
			messageDetails.XML_MsgResponse.setLength(0);
			// возвращаем то, что прислали
			messageDetails.XML_MsgResponse.append( confirmationContent_4_Replace );
		}

		return Function_Result;

	}
	public static int SaveRequest_4_MessageQueue ( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
												   TheadDataAccess theadDataAccess,  Logger MessegeReceive_Log) {

		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);

		XPathExpression<Element> xpathQueue_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/QueryString/x_Queue_Id", Filters.element());
		Element elmtQueue_Id = xpathQueue_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtQueue_Id== null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе SaveRequest4MessageQueue не найден параметр Parametrs/QueryString/Queue_Id");
			return -33;
		}
		String Pk_Value= elmtQueue_Id.getText();
		//String ParamElements[] = Pk_Value.split("-@-");
		long Queue_Id_4_SaveRequest = Long.parseLong(Pk_Value);

		XPathExpression<Element> xpathRequestContent_4_Save = XPathFactory.instance().compile("/Envelope/Body/Parametrs/Data", Filters.element());
		Element elmtRequestContent_4_Save = xpathRequestContent_4_Save.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtRequestContent_4_Save == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе SaveRequest4MessageQueue не найден параметр Parametrs/Data");
			return -36;
		}
		String requestContent_4_Save = elmtRequestContent_4_Save.getText();
		int Function_Result;
		try {
			Function_Result = PerformSaveRequest.SaveRequestBody_4_MessageQueue(
					theadDataAccess, Queue_Id_4_SaveRequest, messageDetails, requestContent_4_Save, MessegeReceive_Log);
		}
		catch (Exception e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(" В системе Java метод:`SaveRequest4MessageQueue при вызове SaveRequestBody_4_MessageQueue=(" + requestContent_4_Save + ") fault" + e.getMessage() );
			Function_Result = -1;
		}
		if ( Function_Result != 0) {
			messageDetails.MsgReason.append(" В системе Java метод:`SaveRequest4MessageQueue");
			messageDetails.MsgReason.append("` не реализован, проконсультируйтесь с разработчиками");
			MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "] В системе Java метод:`SaveRequest4MessageQueue` не реализован, проконсультируйтесь с разработчиками");
		}
		else {
			messageDetails.XML_MsgResponse.setLength(0);
			// возвращаем то, что прислали
			messageDetails.XML_MsgResponse.append( requestContent_4_Save );
		}

		return Function_Result;
		}



	// сохранить конкретную секцию Шаблона MessageTemplates_SaveConfig
	public static int MessageTemplates_SaveConfig( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
											   TheadDataAccess theadDataAccess, String dbSchema , Logger MessegeReceive_Log) {
		//int nn = 0;
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/QueryString/x_Template_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtTemplate_Id == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе MessageTemplates_SaveConfig не найден параметр Parametrs/QueryString/Template_Id");
			return -33;
		}
		String Pk_Value= elmtTemplate_Id.getText();
		//String ParamElements[] = Pk_Value.split("-@-");
		int Template_Id_4_Update_MessageTemplate = Integer.parseInt(Pk_Value);

		XPathExpression<Element> xpathConfigEntry = XPathFactory.instance().compile("/Envelope/Body/Parametrs/QueryString/x_ConfigEntry", Filters.element());
		Element elmtConfigEntry = xpathConfigEntry.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtConfigEntry == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе MessageTemplates_SaveConfig не найден параметр Parametrs/QueryString/ConfigEntry");
			return -34;
		}
		String configEntry = elmtConfigEntry.getText();
		try {
			PerformSaveTemplateEntry.check_Conf_Text( configEntry);
		} catch (Exception e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(e.getMessage());
			e.printStackTrace();
			return -35;
		}
		XPathExpression<Element> xpathContentEntry_4_Save = XPathFactory.instance().compile("/Envelope/Body/Parametrs/Data", Filters.element());
		Element elmtContentEntry_4_Save = xpathContentEntry_4_Save.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtContentEntry_4_Save == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "["+ messageQueueVO.getQueue_Id() +" ] В запросе MessageTemplates_SaveConfig не найден параметр Parametrs/Data");
			return -36;
		}

		String contentEntry_4_Save  = elmtContentEntry_4_Save.getText();;

		try {
			PreparedStatement stmtMsgTemplate = theadDataAccess.Hermes_Connection.prepareStatement(
					"select t.template_id, " +
							"t.interface_id, " +
							"t.operation_id, " +
							"t.msg_type, " +
							"t.msg_type_own, " +
							"t.template_name, " +
							"t.template_dir, " +
							"t.source_id, " +
							"t.destin_id, " +
							"t.conf_text, " +
							"t.src_subcod, " +
							"t.dst_subcod, " +
							"t.lastmaker, " +
							"t.lastdate " +
							"from "+ dbSchema + ".MESSAGE_TemplateS t where (1=1) and t.template_id =?"
			);
			stmtMsgTemplate.setInt(1, Template_Id_4_Update_MessageTemplate );
			ResultSet rs = stmtMsgTemplate.executeQuery();
			while (rs.next()) {
				MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
				messageTemplateVO.setMessageTemplateVO(
						rs.getInt("template_id"),
						rs.getInt("Interface_Id"),
						rs.getInt("Operation_Id"),
						rs.getInt("Source_Id"),
						rs.getString("Src_SubCod"),
						rs.getInt("Destin_Id"),
						rs.getString("Dst_SubCod"),
						rs.getString("Msg_Type"),
						rs.getString("Msg_Type_own"),
						rs.getString("Template_name"),
						rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
						"INFO",
						rs.getString("Conf_Text"),
						rs.getString("LastMaker"),
						rs.getString("LastDate")
				);
				ConfigMsgTemplates.performConfig(messageTemplateVO, MessegeReceive_Log);

				// MessegeReceive_Log.warn( "Conf_Text() before:`{}`" , Config_Text );
				try {
					String Conf_Text =
							PerformSaveTemplateEntry.replace_Conf_Text(messageQueueVO.getQueue_Id(), messageTemplateVO, configEntry, contentEntry_4_Save, MessegeReceive_Log);
					// MessegeReceive_Log.warn("["+ messageQueueVO.getQueue_Id() +" ] Conf_Text() for update [Template_Id=" + Template_Id_4_Update_MessageTemplate + "]:\n" + Conf_Text);
					int result_UPDATE_MessageTemplate =
									theadDataAccess.doUpdate_MESSAGE_Template( messageQueueVO.getQueue_Id(), Template_Id_4_Update_MessageTemplate, Conf_Text,
																				MessegeReceive_Log );
					if (result_UPDATE_MessageTemplate == 0)
						MessegeReceive_Log.warn("["+ messageQueueVO.getQueue_Id() +" ] update Message_Template (`"+ configEntry + "`) for Template_Id =" + Template_Id_4_Update_MessageTemplate + " has been successfully done.");
					messageDetails.MsgReason.append("update Message_Template (`"+ configEntry + "`) for Template_Id =" + Template_Id_4_Update_MessageTemplate + " has been successfully done.");
				}
				catch (Exception e) {
					messageDetails.MsgReason.setLength(0);
					messageDetails.MsgReason.append(e.getMessage());
					e.printStackTrace();
					return -2;
				}
			}
		} catch (SQLException e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(e.getMessage());
			e.printStackTrace();
			return -2;
		}

		messageDetails.XML_MsgResponse.setLength(0);
		// возвращаем то, что прислали
		messageDetails.XML_MsgResponse.append( contentEntry_4_Save );
		return 0;
	}
	// получить конкретную секцию Шаблона для показа в UI
    public static int GetConfig_Text_Template( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
                                                TheadDataAccess theadDataAccess, String dbSchema , Logger MessegeReceive_Log) {
        int nn = 0;
        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;
        messageDetails.MsgReason.setLength(0);
		StringBuilder Config_Text= new StringBuilder();

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Template_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtTemplate_Id == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "В запросе GetConfig_Text_Template не найден параметр Template_Id");
			return -33;
		}
		String Template_Id_Value= elmtTemplate_Id.getText();

		XPathExpression<Element> xpathConfigEntry = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_ConfigEntry", Filters.element());
		Element elmtConfigEntry = xpathConfigEntry.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtConfigEntry == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "В запросе GetConfig_Text_Template не найден параметр ConfigEntry");
			return -32;
		}
		String configEntry = elmtConfigEntry.getText();
		try {
			PreparedStatement stmtMsgTemplate = theadDataAccess.Hermes_Connection.prepareStatement(
				 "select t.template_id, " +
							"t.interface_id, " +
							"t.operation_id, " +
							"t.msg_type, " +
							"t.msg_type_own, " +
							"t.template_name, " +
							"t.template_dir, " +
							"t.source_id, " +
							"t.destin_id, " +
							"t.conf_text, " +
							"t.src_subcod, " +
							"t.dst_subcod, " +
							"t.lastmaker, " +
							"t.lastdate " +
							"from "+ dbSchema + ".MESSAGE_TemplateS t where (1=1) and t.template_id =?"
			);
			stmtMsgTemplate.setInt(1, Integer.parseInt(Template_Id_Value));
			ResultSet rs = stmtMsgTemplate.executeQuery();
			while (rs.next()) {
				MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
				messageTemplateVO.setMessageTemplateVO(
						rs.getInt("template_id"),
						rs.getInt("Interface_Id"),
						rs.getInt("Operation_Id"),
						rs.getInt("Source_Id"),
						rs.getString("Src_SubCod"),
						rs.getInt("Destin_Id"),
						rs.getString("Dst_SubCod"),
						rs.getString("Msg_Type"),
						rs.getString("Msg_Type_own"),
						rs.getString("Template_name"),
						rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
						"INFO",
						rs.getString("Conf_Text"),
						rs.getString("LastMaker"),
						rs.getString("LastDate")
				);
				//Config_Text = rs.getString("Conf_Text");
				ConfigMsgTemplates.performConfig(messageTemplateVO, MessegeReceive_Log);
				// пробуем Чмстый XML
				// Config_Text.append(XMLchars.OpenTag).append(configEntry).append(XMLchars.CloseTag).append("<![CDATA[");
				switch ( configEntry) {
					case "EnvelopeInXSLT":
						Config_Text.append( messageTemplateVO.getEnvelopeInXSLT() );
						break;

					case "HeaderInXSLT":
						Config_Text.append( messageTemplateVO.getHeaderInXSLT() );
						break;
					case "WsdlInterface":
						Config_Text.append( messageTemplateVO.getWsdlInterface() );
						break;
					case "WsdlXSD1":
						Config_Text.append( messageTemplateVO.getWsdlXSD1() );
						break;
					case "WsdlXSD2":
						Config_Text.append( messageTemplateVO.getWsdlXSD2() );
						break;
					case "WsdlXSD3":
						Config_Text.append( messageTemplateVO.getWsdlXSD3() );
						break;
					case "WsdlXSD4":
						Config_Text.append( messageTemplateVO.getWsdlXSD4() );
						break;
					case "WsdlXSD5":
						Config_Text.append( messageTemplateVO.getWsdlXSD5() );
						break;
					case "WsdlXSD6":
						Config_Text.append( messageTemplateVO.getWsdlXSD6() );
						break;
					case "WsdlXSD7":
						Config_Text.append( messageTemplateVO.getWsdlXSD7() );
						break;
					case "WsdlXSD8":
						Config_Text.append( messageTemplateVO.getWsdlXSD8() );
						break;
					case "WsdlXSD9":
						Config_Text.append( messageTemplateVO.getWsdlXSD9() );
						break;

					case "ConfigExecute":
						Config_Text.append( messageTemplateVO.getConfigExecute() );
						break;
					case "MessageXSD":
						Config_Text.append( messageTemplateVO.getMessageXSD() );
						break;
					case "HeaderXSLT":
						Config_Text.append( messageTemplateVO.getHeaderXSLT() );
						break;

					case  "ConfigPostExec":
						Config_Text.append( messageTemplateVO.getConfigPostExec() );

						break;
					case "EnvelopeXSLTPost":
						Config_Text.append( messageTemplateVO.getEnvelopeXSLTPost() );
						break;

					case "MsgAnswXSLT":
						Config_Text.append( messageTemplateVO.getMsgAnswXSLT() );
						break;

					case "MessageXSLT":
						Config_Text.append( messageTemplateVO.getMessageXSLT() );
						break;

					case "AckXSLT":
						Config_Text.append( messageTemplateVO.getAckXSLT() );
						break;

					case "EnvelopeXSLTExt":
						Config_Text.append( messageTemplateVO.getEnvelopeXSLTExt() );
						break;

					case "EnvelopeNS":
						Config_Text.append( messageTemplateVO.getEnvelopeNS() );
						break;
					case "MessageAck":
						Config_Text.append( messageTemplateVO.getMessageAck() );
						break;
					case "MessageAnswAck":
						Config_Text.append( messageTemplateVO.getMessageAnswAck() );
						break;
					case "MessageAnswerXSD":
						Config_Text.append( messageTemplateVO.getMessageAnswerXSD() );
						break;
					case "MessageAnswMsgXSLT":
						Config_Text.append( messageTemplateVO.getMessageAnswMsgXSLT () );
						break;

					case "AckXSD":
						Config_Text.append( messageTemplateVO.getAckXSD() );
						break;
					case "AckAnswXSLT":
						Config_Text.append( messageTemplateVO.getAckAnswXSLT() );
						break;
					case "HeaderXSD":
						Config_Text.append( messageTemplateVO.getHeaderXSD() );
						break;
					case "ErrTransXSLT":
						Config_Text.append( messageTemplateVO.getErrTransXSLT() );
						break;
				}
				//  пробуем Чмстый XML
				// Config_Text.append("]]>").append(XMLchars.OpenTag).append(XMLchars.EndTag).append(configEntry).append(XMLchars.CloseTag);
			}
		} catch (SQLException e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(e.getMessage());
		    e.printStackTrace();
		return -2;
	}
/*
        String Config_Text= 	"<orderStatusNotification>" +
			"<originator>CMS.KKFU</originator>" +
			"<receiver>HRMS</receiver>" +
			"<orderResult>" +
				"<orderResultCode>0</orderResultCode>" +
			"</orderResult>" +
			"<order>" +
			    "<orderId>5356769</orderId>" +
				"<orderOMSId>22-600304-1</orderOMSId>" +
				"<orderState>COMPLETED</orderState>" +
				"<orderCompletionDate>2022-02-17T17:47:26.000+03:00</orderCompletionDate>" +
				"<orderComments>" +
					"<Comment>" +
                        "<text>Заказ создан успешно в CMS КЦ. Ожидает команды отправки на проработку</text>" +
					"</Comment>" +
				"</orderComments>" +
			"</order>" +
		"</orderStatusNotification>"
        ;
*/
        messageDetails.XML_MsgResponse.setLength(0);
        messageDetails.XML_MsgResponse.append( Config_Text );
        return nn;
    }
	// получить список непустых секций для конкретного Шаблона
	public static int GetConfig_Entrys_Template( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
											   TheadDataAccess theadDataAccess,String dbSchema, Logger MessegeReceive_Log) {
		int nn = 0;
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		messageDetails.X_Total_Count=0;
		StringBuilder Config_Text= new StringBuilder("<data>");

		//XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Template_Id", Filters.element());
		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile(" /Envelope/Body/Parametrs/RecordFilters/RecordFilter/RecordFilterFieldValue", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtTemplate_Id == null ) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append("В запросе не найден элемент-параметр: Template_Id");
			return -3;
		}

		String Template_Id_Value= elmtTemplate_Id.getText();
;
		try {
			PreparedStatement stmtMsgTemplate = theadDataAccess.Hermes_Connection.prepareStatement(
					"select t.Conf_Text from " + dbSchema+ ".MESSAGE_TemplateS t where (1=1) and t.template_id =?"
			);
			stmtMsgTemplate.setInt(1, Integer.parseInt(Template_Id_Value));
			ResultSet rs = stmtMsgTemplate.executeQuery();
			while (rs.next()) {
				String parsedConfig = rs.getString("Conf_Text");

				try {
					SAXBuilder documentBuilder = new SAXBuilder();
					//DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					InputStream parsedConfigStream = new ByteArrayInputStream( parsedConfig.getBytes(StandardCharsets.UTF_8));
					Document document = (Document)documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);

					Element  TemplConfig = document.getRootElement();
					List<Element> list = TemplConfig.getChildren();
					// Перебор всех элементов TemplConfig
					for (int i = 0; i < list.size(); i++) {
						Element TemplateElmnt =  list.get(i);

						String configContent = TemplateElmnt.getText();
						if ( configContent.length() > 1 ) {
							Config_Text.append("<Record><Config_Id>");
							Config_Text.append(Template_Id_Value); Config_Text.append("-@-");
							Config_Text.append( TemplateElmnt.getName() );
							Config_Text.append("</Config_Id><Config_Name>");
							Config_Text.append( TemplateElmnt.getName() );
							Config_Text.append("</Config_Name></Record>");
							messageDetails.X_Total_Count += 1;
						}
					}
					Config_Text.append("</data>");
				} catch ( JDOMException |  IOException ex) {
					ex.printStackTrace(System.err);
				}
			}
			rs.close();
			stmtMsgTemplate.close();
		} catch (SQLException e) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append(e.getMessage());
			e.printStackTrace();
			return -2;
		}
/*
        String Config_Text= 	"<orderStatusNotification>" +
			"<originator>CMS.KKFU</originator>" +
			"<receiver>HRMS</receiver>" +
			"<orderResult>" +
				"<orderResultCode>0</orderResultCode>" +
			"</orderResult>" +
			"<order>" +
			    "<orderId>5356769</orderId>" +
				"<orderOMSId>22-600304-1</orderOMSId>" +
				"<orderState>COMPLETED</orderState>" +
				"<orderCompletionDate>2022-02-17T17:47:26.000+03:00</orderCompletionDate>" +
				"<orderComments>" +
					"<Comment>" +
                        "<text>Заказ создан успешно в CMS КЦ. Ожидает команды отправки на проработку</text>" +
					"</Comment>" +
				"</orderComments>" +
			"</order>" +
		"</orderStatusNotification>"
        ;
*/
		messageDetails.XML_MsgResponse.setLength(0);
		messageDetails.XML_MsgResponse.append( Config_Text );
		return nn;
	}
}
