// Code generated from KsqlGrammar.g4 by ANTLR 4.13.1. DO NOT EDIT.

package grammar // KsqlGrammar
import "github.com/antlr4-go/antlr/v4"

type BaseKsqlGrammarVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseKsqlGrammarVisitor) VisitStatements(ctx *StatementsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTestStatement(ctx *TestStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSingleStatement(ctx *SingleStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSingleExpression(ctx *SingleExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitQueryStatement(ctx *QueryStatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListProperties(ctx *ListPropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListTopics(ctx *ListTopicsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListStreams(ctx *ListStreamsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListTables(ctx *ListTablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListFunctions(ctx *ListFunctionsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListConnectors(ctx *ListConnectorsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListConnectorPlugins(ctx *ListConnectorPluginsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListTypes(ctx *ListTypesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListVariables(ctx *ListVariablesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitShowColumns(ctx *ShowColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDescribeStreams(ctx *DescribeStreamsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDescribeFunction(ctx *DescribeFunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDescribeConnector(ctx *DescribeConnectorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitPrintTopic(ctx *PrintTopicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitListQueries(ctx *ListQueriesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTerminateQuery(ctx *TerminateQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSetProperty(ctx *SetPropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitUnsetProperty(ctx *UnsetPropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDefineVariable(ctx *DefineVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitUndefineVariable(ctx *UndefineVariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCreateStream(ctx *CreateStreamContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCreateStreamAs(ctx *CreateStreamAsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCreateTable(ctx *CreateTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCreateTableAs(ctx *CreateTableAsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCreateConnector(ctx *CreateConnectorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitInsertInto(ctx *InsertIntoContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitInsertValues(ctx *InsertValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDropStream(ctx *DropStreamContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDropTable(ctx *DropTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDropConnector(ctx *DropConnectorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitExplain(ctx *ExplainContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitRegisterType(ctx *RegisterTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDropType(ctx *DropTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAlterSource(ctx *AlterSourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAssertValues(ctx *AssertValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAssertTombstone(ctx *AssertTombstoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAssertStream(ctx *AssertStreamContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAssertTable(ctx *AssertTableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitRunScript(ctx *RunScriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitResultMaterialization(ctx *ResultMaterializationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAlterOption(ctx *AlterOptionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTableElements(ctx *TableElementsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTableElement(ctx *TableElementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitColumnConstraints(ctx *ColumnConstraintsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTableProperties(ctx *TablePropertiesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTableProperty(ctx *TablePropertyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitPrintClause(ctx *PrintClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitIntervalClause(ctx *IntervalClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLimitClause(ctx *LimitClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitRetentionClause(ctx *RetentionClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitGracePeriodClause(ctx *GracePeriodClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitWindowExpression(ctx *WindowExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTumblingWindowExpression(ctx *TumblingWindowExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitHoppingWindowExpression(ctx *HoppingWindowExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSessionWindowExpression(ctx *SessionWindowExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitWindowUnit(ctx *WindowUnitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitGroupBy(ctx *GroupByContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitPartitionBy(ctx *PartitionByContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitValues(ctx *ValuesContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSelectSingle(ctx *SelectSingleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSelectAll(ctx *SelectAllContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinRelation(ctx *JoinRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitRelationDefault(ctx *RelationDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinedSource(ctx *JoinedSourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitInnerJoin(ctx *InnerJoinContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitOuterJoin(ctx *OuterJoinContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLeftJoin(ctx *LeftJoinContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinWindow(ctx *JoinWindowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinWindowWithBeforeAndAfter(ctx *JoinWindowWithBeforeAndAfterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSingleJoinWindow(ctx *SingleJoinWindowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinWindowSize(ctx *JoinWindowSizeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitJoinCriteria(ctx *JoinCriteriaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAliasedRelation(ctx *AliasedRelationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitColumns(ctx *ColumnsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTableName(ctx *TableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitExpression(ctx *ExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLogicalNot(ctx *LogicalNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBooleanDefault(ctx *BooleanDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLogicalBinary(ctx *LogicalBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitPredicated(ctx *PredicatedContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitComparison(ctx *ComparisonContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBetween(ctx *BetweenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitInList(ctx *InListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLike(ctx *LikeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitNullPredicate(ctx *NullPredicateContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDistinctFrom(ctx *DistinctFromContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitValueExpressionDefault(ctx *ValueExpressionDefaultContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitConcatenation(ctx *ConcatenationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitArithmeticBinary(ctx *ArithmeticBinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitArithmeticUnary(ctx *ArithmeticUnaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitAtTimeZone(ctx *AtTimeZoneContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDereference(ctx *DereferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSimpleCase(ctx *SimpleCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitColumnReference(ctx *ColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSubscript(ctx *SubscriptContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitStructConstructor(ctx *StructConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTypeConstructor(ctx *TypeConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitQualifiedColumnReference(ctx *QualifiedColumnReferenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitCast(ctx *CastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitParenthesizedExpression(ctx *ParenthesizedExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitArrayConstructor(ctx *ArrayConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitMapConstructor(ctx *MapConstructorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitFunctionCall(ctx *FunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSearchedCase(ctx *SearchedCaseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLiteralExpression(ctx *LiteralExpressionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitFunctionArgument(ctx *FunctionArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTimeZoneString(ctx *TimeZoneStringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitComparisonOperator(ctx *ComparisonOperatorContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBooleanValue(ctx *BooleanValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitType(ctx *TypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitTypeParameter(ctx *TypeParameterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBaseType(ctx *BaseTypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitWhenClause(ctx *WhenClauseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitVariableIdentifier(ctx *VariableIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitUnquotedIdentifier(ctx *UnquotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDigitIdentifier(ctx *DigitIdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitLambda(ctx *LambdaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitVariableName(ctx *VariableNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitVariableValue(ctx *VariableValueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitSourceName(ctx *SourceNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitFloatLiteral(ctx *FloatLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitNullLiteral(ctx *NullLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitNumericLiteral(ctx *NumericLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitVariableLiteral(ctx *VariableLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseKsqlGrammarVisitor) VisitNonReserved(ctx *NonReservedContext) interface{} {
	return v.VisitChildren(ctx)
}
