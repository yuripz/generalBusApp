package net.plumbing.msgbus.threads.utils;
//import oracle.jdbc.OracleCallableStatement;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplateVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
//import oracle.jdbc.OracleResultSetMetaData;
//import oracle.jdbc.OracleTypes;
import org.apache.commons.text.StringEscapeUtils;
import org.jdom2.Attribute;
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
import java.util.HashMap;
import java.util.List;
import com.github.underscore.U;

import static net.plumbing.msgbus.common.ClientIpHelper.toCamelCase;
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

	public static int NestedLoopSQLIncludeXML(TheadDataAccess theadDataAccess,
											boolean isExtSystemAccess,
											Connection extSystemDataConnection,
											String Passed_Envelope4XSLTPost,
											MessageQueueVO messageQueueVO,
											MessageDetails messageDetails, boolean isDebugged, Logger MessegeSend_Log) {
		// эксперементальная непротестированная функция для возможности реализаций master-detail или обращений к другому экземпляру БД
		int nn = 0;
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		// messageDetails.MsgReason.append("ExecuteSQLinXML is not ready yet! ");
		Connection current_Connection_4_ExecuteSQL;
		if ( ( isExtSystemAccess ) && ( extSystemDataConnection != null ))
			current_Connection_4_ExecuteSQL = extSystemDataConnection;
		else
			current_Connection_4_ExecuteSQL = theadDataAccess.Hermes_Connection;

		PreparedStatement selectStatement = null;
		String SQLcallableStatementExpression=null;
		//final String SQLparamValue;
		//String SQLStatement_functionORselect= OperTypeSel;
		String SQLStatement_ColumnCount;

		String  SubSelectStatementStatement = null;
		int firstStatementParamNum;
		HashMap<Integer, String > SQLparamValues = new HashMap<Integer, String >(); // SQLparamValues.clear();
		// List <String> SQLparamValues= Arrays.asList();

		StringBuffer msg_Reason = new StringBuffer();
		//final
		// boolean isDebugged=    true; // false; //
		int numSQLStatement_founded = 0;

		try {
			SAXBuilder documentBuilder = new SAXBuilder();
			InputStream parsedMessageStream = new ByteArrayInputStream(Passed_Envelope4XSLTPost.getBytes(StandardCharsets.UTF_8));
			Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);
			try {
				// /SQLRequest/SQLStatement
				Element SQLRequest = document.getRootElement();
				List<Element> SQLRequestList = SQLRequest.getChildren();
				// Перебор всех элементов SQLStatement
				for (int sqlRequestListIndex = 0; sqlRequestListIndex < SQLRequestList.size(); sqlRequestListIndex++) {
					// Ищем элнмент  == SQLStatement
					Element SQLStatement = SQLRequestList.get(sqlRequestListIndex);
					// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML:.getRootElement: SQLRequestList.get(" + sqlRequestListIndex + ") SQLStatement.getName()= (" + SQLStatement.getName() + ")");
					if (SQLStatement.getName().equals( TagNameSQLStatement )) {
						numSQLStatement_founded = numSQLStatement_founded +1;
						Attribute SQLStatement_Type = SQLStatement.getAttribute(AttrNameStateType);
						//SQLStatement_functionORselect = SQLStatement_Type.getValue();
						 firstStatementParamNum = 0;

						Attribute SQLStatement_ReturnColumnCount = SQLStatement.getAttribute(AttrNameStateNum);
						SQLStatement_ColumnCount = SQLStatement_ReturnColumnCount.getValue();

						if (isDebugged)
							MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML:.SQLStatement.getAttribute(SQLStatement_Type)=`" + SQLStatement_Type.getValue() + "` =================================");
						// SQLStatement_Type.getValue();
						// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] =============================================================================================");
						List<Element> SQLStatementParamList = SQLStatement.getChildren();
						// Перебор всех элементов SQLStatement
						for ( int sqlStatementParamListIndex = 0; sqlStatementParamListIndex < SQLStatementParamList.size(); sqlStatementParamListIndex++) {
							Element SQLStatementParam = SQLStatementParamList.get(sqlStatementParamListIndex);
							// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML: SQLStatementParamList.get(" + sqlStatementParamListIndex + ") SQLStatementParam.getName()= (" + SQLStatementParam.getName() + "), SQLStatementParam.value=(" + SQLStatementParam.getTextTrim() + ")");
							//if (isDebugged) MessegeSend_Log.warn("-------------------------------------------------------------------------------------------------------");
							if (SQLStatementParam.getName().equals(TagNamePSTMT))
								SQLcallableStatementExpression = SQLStatementParam.getText();

							Attribute SQLStatementParam_Type;
							if (SQLStatementParam.getName().equals(TagNameParam)) {
								List <Attribute>  SQLStatementParamAttributes = SQLStatementParam.getAttributes();
								// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML: SQLStatementParamAttributes.size = " + SQLStatementParamAttributes.size() );

								for (int j = 0; j < SQLStatementParamAttributes.size(); j++)
								{   SQLStatementParam_Type = SQLStatementParamAttributes.get(j);
									// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML=> getAttribute: SQLStatementParam_Type.getName(" + SQLStatementParam_Type.getName() + ")=" + SQLStatementParam_Type.getValue() );

									if ( SQLStatementParam_Type.getName().equals( AttrNameParamNum )) {
										// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] ExecuteSQLincludedXML-> SQLStatementParam.getName(" + SQLStatementParam.getName() + "), " + "SQLStatementParam.value=(" + SQLStatementParam.getTextTrim() + ")," + " getAttribute: SQLStatementParam_Type.getName(" + SQLStatementParam_Type.getName() + ")=" + SQLStatementParam_Type.getValue() );
										int NN = Integer.parseInt(SQLStatementParam_Type.getValue());
										if ( NN > firstStatementParamNum ) {
											SQLparamValues.put(NN-firstStatementParamNum-1, SQLStatementParam.getTextTrim()); //
											//String sss = SQLStatementParam.getValue();
											// if (isDebugged) MessegeSend_Log.warn(" ExecuteSQLincludedXML-> put(" + (NN-firstStatementParamNum-1) +") [" +SQLStatementParam.getTextTrim() + " ]");
											//MessegeSend_Log.warn(" ExecuteSQLincludedXML-> sss-put(" + (NN-firstStatementParamNum-1) +") [" + sss + " ]");
										}
									}
								}
                                /*
                                SQLStatementParam_Type = SQLStatement.getAttribute(AttrNameParamNum);
                                if ( SQLStatementParam_Type !=null) {
                                    SQLparamValues.add(SQLStatementParam_Type.getValue() );
                                    MessegeSend_Log.warn(" ExecuteSQLincludedXML.getAttribute: SQLStatementParam.getName()= (" + SQLStatementParam.getName() + "), " + SQLStatementParam_Type.getName() + "=" + SQLStatementParam_Type.getValue() );

                                }
                                */
							}
							// if (isDebugged) MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] -------------------------------------------------------------------------------------------------------");
						}
					} // нашли SQLStatement

					if (SQLStatement.getName().equals( TagNameSubSelectStatement )) {
						SubSelectStatementStatement = SQLStatement.getText();
					}
				}

				if ( SQLcallableStatementExpression == null )
				{
					messageDetails.MsgReason.append("ExecuteSQLincludedXML: Не нашли " + "/"+ TagNameHead+ "/"+ TagNameSQLStatement + "/" + TagNamePSTMT + " в результате XSLT прообразования " + Passed_Envelope4XSLTPost);
					MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] "+ messageDetails.MsgReason.toString());
					return -2;
				}
/*************************************************************************
 String xpathSQLStatementParamExpression = "/"+ TagNameHead +"/"+ TagNameStatement + "/Param[2]"; ///SQLRequest/SQLStatement/Param[2]
 Integer iMsgStaus = 1233;

 XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathSQLStatementParamExpression, Filters.element());
 Element emtMessage = xpathMessage.evaluateFirst(document);
 if ( emtMessage != null ) {
 SQLparamValue = emtMessage.getText();
 messageDetails.MsgReason.append(" Param=" + SQLparamValue );
 }
 else {
 messageDetails.MsgReason.append("Не нашли " + xpathSQLStatementParamExpression + " в " + Passed_Envelope4XSLTPost);
 MessegeSend_Log.error(messageDetails.MsgReason.toString());
 return -2;
 }
 /////////////////////////////**********************************************/

				if (isDebugged) {
					for ( int k =0; k < SQLparamValues.size(); k++ )
						MessegeSend_Log.warn( "SQLparamValues.get(" + k + " )=" + SQLparamValues.get(k).toString());
				}
				if (isDebugged)  MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] =====================================================================================");
				// todo
				// boolean is_NoConfirmation =
				//        MessageRepositoryHelper.isNoConfirmation4MessageTypeURL_SOAP_Ack_2_Operation(  messageQueueVO.getOperation_Id(), MessegeSend_Log );
				// только SELECT из таблиц
					try {
						// Step 2.B: Creating JDBC selectStatement одинково для обоих случаев
						selectStatement = current_Connection_4_ExecuteSQL.prepareStatement (SQLcallableStatementExpression);
						msg_Reason.append( SQLcallableStatementExpression);
						if (isDebugged)
							MessegeSend_Log.info( SQLcallableStatementExpression );
						// register OUT parameter
						if (! SQLparamValues.isEmpty() )
							msg_Reason.append( " using: " );

						for ( int k =0; k < SQLparamValues.size(); k++ ) {
							if (isDebugged)
								MessegeSend_Log.warn("selectStatement.setString: SQLparamValues.get(" + 1+k + " )=" + SQLparamValues.get(k));
							selectStatement.setString(1 + k, SQLparamValues.get(k));
							msg_Reason.append( SQLparamValues.get(k)); msg_Reason.append( ", " );
						}

						try {
							// Step 2.C: Executing Select-Statement
							ResultSet rs = selectStatement.executeQuery();
							ResultSetMetaData ResultSetMetaData = selectStatement.getMetaData();
							int ColumnCount = ResultSetMetaData.getColumnCount();
							int i;
							if (isDebugged)
								for (i=1; i < ColumnCount+1; i++ ) {
									MessegeSend_Log.warn(
											"ColumnType: " + ResultSetMetaData.getColumnType(i) +
													" , ColumnName: " + ResultSetMetaData.getColumnName(i) +
													" , ColumnTypeName: " + ResultSetMetaData.getColumnTypeName(i) +
													" , ColumnLabel: " + ResultSetMetaData.getColumnLabel(i)
									);
								}
							int num_Rows4Perform = 0;
							// Формируем псевдо XML_MsgConfirmation из PIPEfunction
							messageDetails.XML_MsgConfirmation.setLength(0);

								messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.TagConfirmation + XMLchars.CloseTag // <Confirmation>
												+ XMLchars.OpenTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag //  <ResultCode>
												+ "0"
												+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameTagFaultResult + XMLchars.CloseTag // </ResultCode>
												+ XMLchars.OpenTag + XMLchars.TagDetailList + XMLchars.CloseTag //  <DetailList>
										// + XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag // не ЗАКРЫВАЕМ  </DetailList>
										//+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // не ЗАКРЫВАЕМ </Confirmation>
								);
								String ColumnLabel;
								while (rs.next()) {
									messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + RowTag + XMLchars.CloseTag //  <Record>
									);
									num_Rows4Perform += 1;
									for (i = 1; i < ColumnCount + 1; i++) {
										ColumnLabel = toCamelCase( ResultSetMetaData.getColumnLabel(i), "_" );
										messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag);
										messageDetails.XML_MsgConfirmation.append( ColumnLabel); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
										messageDetails.XML_MsgConfirmation.append( StringEscapeUtils.escapeXml10(rs.getString(i)) )   ;
										messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag); messageDetails.XML_MsgConfirmation.append(XMLchars.EndTag);
										messageDetails.XML_MsgConfirmation.append( ColumnLabel ); messageDetails.XML_MsgConfirmation.append(XMLchars.CloseTag);
									}
									messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + RowTag + XMLchars.CloseTag //  </Record>
									);
									num_Rows4Perform += 1;
								} // Цикл по выборке

								messageDetails.XML_MsgConfirmation.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagDetailList + XMLchars.CloseTag //   </DetailList>
										+ XMLchars.OpenTag + XMLchars.EndTag + XMLchars.TagConfirmation + XMLchars.CloseTag // </Confirmation>
								);

							rs.close();
							if (isDebugged)
								MessegeSend_Log.warn( messageDetails.XML_MsgConfirmation.toString() );
							if (isDebugged)
								MessegeSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] num_Rows4Perform=" + num_Rows4Perform );

						} catch (SQLException e) {
							//e.printStackTrace();
							messageDetails.MsgReason.append(", SQLException selectStatement.executeQuery(`");
							messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10(SQLcallableStatementExpression) );
							messageDetails.MsgReason.append("`):="); messageDetails.MsgReason.append( e.getMessage()); //sStackTrace.strInterruptedException(e));
							MessegeSend_Log.error(messageDetails.MsgReason.toString());
							selectStatement.close();
							current_Connection_4_ExecuteSQL.rollback();
							return -3;
						}

						// get count and print in console
						selectStatement.close();
						// Устанавливаеи признак завершения работы прикладного обработчика  == "EXEIN"
						int result = theadDataAccess.doUPDATE_MessageQueue_IN2ExeIN(messageQueueVO.getQueue_Id(),
								msg_Reason.toString(),  MessegeSend_Log);
						messageQueueVO.setQueue_Direction(XMLchars.DirectEXEIN);
						current_Connection_4_ExecuteSQL.commit();
					} catch (SQLException e) {
						messageDetails.MsgReason.append( OperTypeSel + " SQLException Hermes_Connection.prepareCall:=`"); messageDetails.MsgReason.append( SQLcallableStatementExpression); messageDetails.MsgReason.append("`"); messageDetails.MsgReason.append( sStackTrace.strInterruptedException(e) );
						current_Connection_4_ExecuteSQL.rollback();
						MessegeSend_Log.error(messageDetails.MsgReason.toString());
						return -2;
					}
				////------------- SelectTotalStatement ----------------
				if ( SubSelectStatementStatement != null )
				{
					try { selectStatement = current_Connection_4_ExecuteSQL.prepareStatement (SubSelectStatementStatement);
						msg_Reason.append( SubSelectStatementStatement);
						if (isDebugged)
							MessegeSend_Log.info( "[" + messageQueueVO.getQueue_Id() + "] SelectTotalStatement: " + SubSelectStatementStatement );
						ResultSet rs = selectStatement.executeQuery();
						while (rs.next()) {
							messageDetails.X_Total_Count = rs.getInt(1);
						}
						rs.close();
						selectStatement.close();

					} catch (SQLException e) {

						messageDetails.MsgReason.append( OperTypeSel +  " SelectTotal SQLException Hermes_Connection.prepareCall(`");
						messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10(SubSelectStatementStatement)); messageDetails.MsgReason.append("`" );
						messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(e)) );
						MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] "+ messageDetails.MsgReason.toString());
						selectStatement.close();
						current_Connection_4_ExecuteSQL.rollback();
						return -2;
					}
				}

			} catch (Exception ex) {
				// ex.printStackTrace(System.err);
				messageDetails.MsgReason.setLength(0);
				messageDetails.MsgReason.append("ExecuteSQLincludedXML.XPathFactory.xpath.evaluateFirst fault: "); messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(ex)) );
				MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] "+ messageDetails.MsgReason.toString());
				return -1;
			}
		}catch (JDOMException | IOException ex) {
			//ex.printStackTrace(System.err);
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append("ExecuteSQLincludedXML.documentBuilder fault: "); messageDetails.MsgReason.append( StringEscapeUtils.escapeXml10 ( sStackTrace.strInterruptedException(ex)) );
			MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] "+ messageDetails.MsgReason.toString());
			return -4;
		}

/******************** ПОТОМ
 try {
 theadDataAccess.stmtMsgQueueDet.setLong(1, messageQueueVO.getQueue_Id());
 ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
 while (rs.next()) {
 MessageDetailVO messageDetailVO = new MessageDetailVO();
 messageDetailVO.setMessageQueue(
 rs.getString("Tag_Id"),
 rs.getString("Tag_Value"),
 rs.getInt("Tag_Num"),
 rs.getInt("Tag_Par_Num")
 );
 messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
 messageDetails.MessageRowNum += 1;
 // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");

 }
 } catch (SQLException e) {
 MessegeSend_Log.error("Queue_Id=[" + messageQueueVO.getQueue_Id() + "] :" + sStackTrace.strInterruptedException(e));
 e.printStackTrace();
 return nn;
 }
 ************************************************************/
		return nn;
	}

	public static int GetRequest_Body4Message(MessageQueueVO messageQueueVO, MessageDetails messageDetails,
                                              TheadDataAccess theadDataAccess, Logger MessageSend_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		messageDetails.XML_MsgResponse.setLength(0);
		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Queue_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String Queue_Id_Value= elmtTemplate_Id.getText();
		long Queue_Id= Long.parseLong(Queue_Id_Value);
		/*messageDetails.XML_MsgResponse.append( "<?xml version=\"1.0\" encoding=\"UTF-8\"?><SaveGeoObjectResponse>\n" +
				"\t<ResponseCode>10</ResponseCode><ResponseMessage>106891449</ResponseMessage>\n" +
				"</SaveGeoObjectResponse>" );
		*/
		boolean IsDebugged = false; //true;
		int nn = MessageUtils.ReadMessage( theadDataAccess,  Queue_Id,  messageDetails, IsDebugged, MessageSend_Log);
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
											   TheadDataAccess theadDataAccess,Logger MessageSend_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_Queue_Id", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String Queue_Id_Value= elmtTemplate_Id.getText();
		long Queue_Id= Long.parseLong(Queue_Id_Value);

		int nn = MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, messageDetails, MessageSend_Log);
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
													   TheadDataAccess theadDataAccess, String dbSchema, Logger MessageSend_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		boolean isDebugged=true;

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_RowId", Filters.element());
		Element elmtRow_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String QueueLog_RowId_Value= elmtRow_Id.getText();
		// RowId ROWID_QUEUElog= RowId...parseLong(QueueLog_RowId_Value);
		final String SELECT_QUEUElog_Response="select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID = ?";
		// PreparedStatement stmt_SELECT_QUEUElog_Response;
		//String MessageQueueLogResponse = null;
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
					MessageSend_Log.info( "["+ messageQueueVO.getQueue_Id() +"] select Request from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "'");
			}
			rs.close();
			stmt_SELECT_QUEUElog_Response.close();
			stmt_SELECT_QUEUElog_Response = null;

		}catch (SQLException e) {
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
												  TheadDataAccess theadDataAccess, String dbSchema, Logger MessageSend_Log) {
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		boolean isDebugged=true;

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/x_RowId", Filters.element());
		Element elmtRow_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		String QueueLog_RowId_Value= elmtRow_Id.getText();
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
					MessageSend_Log.info( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "'");

			}
			rs.close();
			stmt_SELECT_QUEUElog_Response.close();
			stmt_SELECT_QUEUElog_Response = null;

		}catch (SQLException e) {
			MessageSend_Log.error( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage());
			System.err.println( "["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage());
			messageDetails.XML_MsgResponse.append("["+ messageQueueVO.getQueue_Id() +"] select Response from " + dbSchema + ".MESSAGE_QUEUElog where ROWID ='" + QueueLog_RowId_Value + "' fault: " + e.getMessage() );
			e.printStackTrace();
			if ( stmt_SELECT_QUEUElog_Response != null )
				try { stmt_SELECT_QUEUElog_Response.close(); } catch (SQLException ex) { ex.printStackTrace(); }
			return -1;
		}

			return 0;
	}
	// сохранить конкретную секцию Шаблона MessageTemplates_SaveConfig
	public static int MessageTemplates_SaveConfig( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
											   TheadDataAccess theadDataAccess, String dbSchema , Logger MessageSend_Log) {
		int nn = 0;
		messageDetails.Message.clear();
		messageDetails.MessageRowNum = 0;
		messageDetails.Message_Tag_Num = 0;
		messageDetails.MsgReason.setLength(0);
		StringBuilder Config_Text= new StringBuilder();

		XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile("/Envelope/Body/Parametrs/QueryString/Pk_Value", Filters.element());
		Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(messageDetails.Input_Clear_XMLDocument); // формируется в XMLutils.makeMessageDetailsRestApi на приёме
		if ( elmtTemplate_Id == null) {
			messageDetails.MsgReason.setLength(0);
			messageDetails.MsgReason.append( "В запросе MessageTemplates_SaveConfig не найден параметр QueryString/Pk_Value");
			return -33;
		}
		String Pk_Value= elmtTemplate_Id.getText();
		String ParamElements[] = Pk_Value.split("-@-");

		String configEntry = ParamElements[1];
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
			stmtMsgTemplate.setInt(1, Integer.parseInt(ParamElements[0]));
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
				ConfigMsgTemplates.performConfig(messageTemplateVO, MessageSend_Log);
				Config_Text.append(XMLchars.OpenTag).append(configEntry).append(XMLchars.CloseTag).append("<![CDATA[");
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
				Config_Text.append("]]>").append(XMLchars.OpenTag).append(XMLchars.EndTag).append(configEntry).append(XMLchars.CloseTag);
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
	// получить конкретную секцию Шаблона для показа в UI
    public static int GetConfig_Text_Template( MessageQueueVO messageQueueVO, MessageDetails messageDetails,
                                                TheadDataAccess theadDataAccess, String dbSchema , Logger MessageSend_Log) {
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
				ConfigMsgTemplates.performConfig(messageTemplateVO, MessageSend_Log);
				Config_Text.append(XMLchars.OpenTag).append(configEntry).append(XMLchars.CloseTag).append("<![CDATA[");
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
				Config_Text.append("]]>").append(XMLchars.OpenTag).append(XMLchars.EndTag).append(configEntry).append(XMLchars.CloseTag);
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
											   TheadDataAccess theadDataAccess,String dbSchema, Logger MessageSend_Log) {
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
