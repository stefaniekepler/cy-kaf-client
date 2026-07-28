// Code generated from KsqlGrammar.g4 by ANTLR 4.13.1. DO NOT EDIT.

package grammar // KsqlGrammar
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by KsqlGrammarParser.
type KsqlGrammarVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by KsqlGrammarParser#statements.
	VisitStatements(ctx *StatementsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#testStatement.
	VisitTestStatement(ctx *TestStatementContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#singleStatement.
	VisitSingleStatement(ctx *SingleStatementContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#singleExpression.
	VisitSingleExpression(ctx *SingleExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#queryStatement.
	VisitQueryStatement(ctx *QueryStatementContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listProperties.
	VisitListProperties(ctx *ListPropertiesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listTopics.
	VisitListTopics(ctx *ListTopicsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listStreams.
	VisitListStreams(ctx *ListStreamsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listTables.
	VisitListTables(ctx *ListTablesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listFunctions.
	VisitListFunctions(ctx *ListFunctionsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listConnectors.
	VisitListConnectors(ctx *ListConnectorsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listConnectorPlugins.
	VisitListConnectorPlugins(ctx *ListConnectorPluginsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listTypes.
	VisitListTypes(ctx *ListTypesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listVariables.
	VisitListVariables(ctx *ListVariablesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#showColumns.
	VisitShowColumns(ctx *ShowColumnsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#describeStreams.
	VisitDescribeStreams(ctx *DescribeStreamsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#describeFunction.
	VisitDescribeFunction(ctx *DescribeFunctionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#describeConnector.
	VisitDescribeConnector(ctx *DescribeConnectorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#printTopic.
	VisitPrintTopic(ctx *PrintTopicContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#listQueries.
	VisitListQueries(ctx *ListQueriesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#terminateQuery.
	VisitTerminateQuery(ctx *TerminateQueryContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#setProperty.
	VisitSetProperty(ctx *SetPropertyContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#unsetProperty.
	VisitUnsetProperty(ctx *UnsetPropertyContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#defineVariable.
	VisitDefineVariable(ctx *DefineVariableContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#undefineVariable.
	VisitUndefineVariable(ctx *UndefineVariableContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#createStream.
	VisitCreateStream(ctx *CreateStreamContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#createStreamAs.
	VisitCreateStreamAs(ctx *CreateStreamAsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#createTable.
	VisitCreateTable(ctx *CreateTableContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#createTableAs.
	VisitCreateTableAs(ctx *CreateTableAsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#createConnector.
	VisitCreateConnector(ctx *CreateConnectorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#insertInto.
	VisitInsertInto(ctx *InsertIntoContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#insertValues.
	VisitInsertValues(ctx *InsertValuesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#dropStream.
	VisitDropStream(ctx *DropStreamContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#dropTable.
	VisitDropTable(ctx *DropTableContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#dropConnector.
	VisitDropConnector(ctx *DropConnectorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#explain.
	VisitExplain(ctx *ExplainContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#registerType.
	VisitRegisterType(ctx *RegisterTypeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#dropType.
	VisitDropType(ctx *DropTypeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#alterSource.
	VisitAlterSource(ctx *AlterSourceContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#assertValues.
	VisitAssertValues(ctx *AssertValuesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#assertTombstone.
	VisitAssertTombstone(ctx *AssertTombstoneContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#assertStream.
	VisitAssertStream(ctx *AssertStreamContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#assertTable.
	VisitAssertTable(ctx *AssertTableContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#runScript.
	VisitRunScript(ctx *RunScriptContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#resultMaterialization.
	VisitResultMaterialization(ctx *ResultMaterializationContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#alterOption.
	VisitAlterOption(ctx *AlterOptionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tableElements.
	VisitTableElements(ctx *TableElementsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tableElement.
	VisitTableElement(ctx *TableElementContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#columnConstraints.
	VisitColumnConstraints(ctx *ColumnConstraintsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tableProperties.
	VisitTableProperties(ctx *TablePropertiesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tableProperty.
	VisitTableProperty(ctx *TablePropertyContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#printClause.
	VisitPrintClause(ctx *PrintClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#intervalClause.
	VisitIntervalClause(ctx *IntervalClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#limitClause.
	VisitLimitClause(ctx *LimitClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#retentionClause.
	VisitRetentionClause(ctx *RetentionClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#gracePeriodClause.
	VisitGracePeriodClause(ctx *GracePeriodClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#windowExpression.
	VisitWindowExpression(ctx *WindowExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tumblingWindowExpression.
	VisitTumblingWindowExpression(ctx *TumblingWindowExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#hoppingWindowExpression.
	VisitHoppingWindowExpression(ctx *HoppingWindowExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#sessionWindowExpression.
	VisitSessionWindowExpression(ctx *SessionWindowExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#windowUnit.
	VisitWindowUnit(ctx *WindowUnitContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#groupBy.
	VisitGroupBy(ctx *GroupByContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#partitionBy.
	VisitPartitionBy(ctx *PartitionByContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#values.
	VisitValues(ctx *ValuesContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#selectSingle.
	VisitSelectSingle(ctx *SelectSingleContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#selectAll.
	VisitSelectAll(ctx *SelectAllContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinRelation.
	VisitJoinRelation(ctx *JoinRelationContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#relationDefault.
	VisitRelationDefault(ctx *RelationDefaultContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinedSource.
	VisitJoinedSource(ctx *JoinedSourceContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#innerJoin.
	VisitInnerJoin(ctx *InnerJoinContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#outerJoin.
	VisitOuterJoin(ctx *OuterJoinContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#leftJoin.
	VisitLeftJoin(ctx *LeftJoinContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinWindow.
	VisitJoinWindow(ctx *JoinWindowContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinWindowWithBeforeAndAfter.
	VisitJoinWindowWithBeforeAndAfter(ctx *JoinWindowWithBeforeAndAfterContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#singleJoinWindow.
	VisitSingleJoinWindow(ctx *SingleJoinWindowContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinWindowSize.
	VisitJoinWindowSize(ctx *JoinWindowSizeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#joinCriteria.
	VisitJoinCriteria(ctx *JoinCriteriaContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#aliasedRelation.
	VisitAliasedRelation(ctx *AliasedRelationContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#columns.
	VisitColumns(ctx *ColumnsContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#tableName.
	VisitTableName(ctx *TableNameContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#expression.
	VisitExpression(ctx *ExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#logicalNot.
	VisitLogicalNot(ctx *LogicalNotContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#booleanDefault.
	VisitBooleanDefault(ctx *BooleanDefaultContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#logicalBinary.
	VisitLogicalBinary(ctx *LogicalBinaryContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#predicated.
	VisitPredicated(ctx *PredicatedContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#comparison.
	VisitComparison(ctx *ComparisonContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#between.
	VisitBetween(ctx *BetweenContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#inList.
	VisitInList(ctx *InListContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#like.
	VisitLike(ctx *LikeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#nullPredicate.
	VisitNullPredicate(ctx *NullPredicateContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#distinctFrom.
	VisitDistinctFrom(ctx *DistinctFromContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#valueExpressionDefault.
	VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#concatenation.
	VisitConcatenation(ctx *ConcatenationContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#arithmeticBinary.
	VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#arithmeticUnary.
	VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#atTimeZone.
	VisitAtTimeZone(ctx *AtTimeZoneContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#dereference.
	VisitDereference(ctx *DereferenceContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#simpleCase.
	VisitSimpleCase(ctx *SimpleCaseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#columnReference.
	VisitColumnReference(ctx *ColumnReferenceContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#subscript.
	VisitSubscript(ctx *SubscriptContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#structConstructor.
	VisitStructConstructor(ctx *StructConstructorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#typeConstructor.
	VisitTypeConstructor(ctx *TypeConstructorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#qualifiedColumnReference.
	VisitQualifiedColumnReference(ctx *QualifiedColumnReferenceContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#cast.
	VisitCast(ctx *CastContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#parenthesizedExpression.
	VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#arrayConstructor.
	VisitArrayConstructor(ctx *ArrayConstructorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#mapConstructor.
	VisitMapConstructor(ctx *MapConstructorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#functionCall.
	VisitFunctionCall(ctx *FunctionCallContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#searchedCase.
	VisitSearchedCase(ctx *SearchedCaseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#literalExpression.
	VisitLiteralExpression(ctx *LiteralExpressionContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#functionArgument.
	VisitFunctionArgument(ctx *FunctionArgumentContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#timeZoneString.
	VisitTimeZoneString(ctx *TimeZoneStringContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#comparisonOperator.
	VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#booleanValue.
	VisitBooleanValue(ctx *BooleanValueContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#type.
	VisitType(ctx *TypeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#typeParameter.
	VisitTypeParameter(ctx *TypeParameterContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#baseType.
	VisitBaseType(ctx *BaseTypeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#whenClause.
	VisitWhenClause(ctx *WhenClauseContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#variableIdentifier.
	VisitVariableIdentifier(ctx *VariableIdentifierContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#unquotedIdentifier.
	VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#quotedIdentifierAlternative.
	VisitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#backQuotedIdentifier.
	VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#digitIdentifier.
	VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#lambda.
	VisitLambda(ctx *LambdaContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#variableName.
	VisitVariableName(ctx *VariableNameContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#variableValue.
	VisitVariableValue(ctx *VariableValueContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#sourceName.
	VisitSourceName(ctx *SourceNameContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#decimalLiteral.
	VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#floatLiteral.
	VisitFloatLiteral(ctx *FloatLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#integerLiteral.
	VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#nullLiteral.
	VisitNullLiteral(ctx *NullLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#numericLiteral.
	VisitNumericLiteral(ctx *NumericLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#booleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#stringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#variableLiteral.
	VisitVariableLiteral(ctx *VariableLiteralContext) interface{}

	// Visit a parse tree produced by KsqlGrammarParser#nonReserved.
	VisitNonReserved(ctx *NonReservedContext) interface{}
}
