// Code generated from KsqlGrammar.g4 by ANTLR 4.13.1. DO NOT EDIT.

package grammar // KsqlGrammar
import "github.com/antlr4-go/antlr/v4"

// BaseKsqlGrammarListener is a complete listener for a parse tree produced by KsqlGrammarParser.
type BaseKsqlGrammarListener struct{}

var _ KsqlGrammarListener = &BaseKsqlGrammarListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseKsqlGrammarListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseKsqlGrammarListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseKsqlGrammarListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseKsqlGrammarListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterStatements is called when production statements is entered.
func (s *BaseKsqlGrammarListener) EnterStatements(ctx *StatementsContext) {}

// ExitStatements is called when production statements is exited.
func (s *BaseKsqlGrammarListener) ExitStatements(ctx *StatementsContext) {}

// EnterTestStatement is called when production testStatement is entered.
func (s *BaseKsqlGrammarListener) EnterTestStatement(ctx *TestStatementContext) {}

// ExitTestStatement is called when production testStatement is exited.
func (s *BaseKsqlGrammarListener) ExitTestStatement(ctx *TestStatementContext) {}

// EnterSingleStatement is called when production singleStatement is entered.
func (s *BaseKsqlGrammarListener) EnterSingleStatement(ctx *SingleStatementContext) {}

// ExitSingleStatement is called when production singleStatement is exited.
func (s *BaseKsqlGrammarListener) ExitSingleStatement(ctx *SingleStatementContext) {}

// EnterSingleExpression is called when production singleExpression is entered.
func (s *BaseKsqlGrammarListener) EnterSingleExpression(ctx *SingleExpressionContext) {}

// ExitSingleExpression is called when production singleExpression is exited.
func (s *BaseKsqlGrammarListener) ExitSingleExpression(ctx *SingleExpressionContext) {}

// EnterQueryStatement is called when production queryStatement is entered.
func (s *BaseKsqlGrammarListener) EnterQueryStatement(ctx *QueryStatementContext) {}

// ExitQueryStatement is called when production queryStatement is exited.
func (s *BaseKsqlGrammarListener) ExitQueryStatement(ctx *QueryStatementContext) {}

// EnterListProperties is called when production listProperties is entered.
func (s *BaseKsqlGrammarListener) EnterListProperties(ctx *ListPropertiesContext) {}

// ExitListProperties is called when production listProperties is exited.
func (s *BaseKsqlGrammarListener) ExitListProperties(ctx *ListPropertiesContext) {}

// EnterListTopics is called when production listTopics is entered.
func (s *BaseKsqlGrammarListener) EnterListTopics(ctx *ListTopicsContext) {}

// ExitListTopics is called when production listTopics is exited.
func (s *BaseKsqlGrammarListener) ExitListTopics(ctx *ListTopicsContext) {}

// EnterListStreams is called when production listStreams is entered.
func (s *BaseKsqlGrammarListener) EnterListStreams(ctx *ListStreamsContext) {}

// ExitListStreams is called when production listStreams is exited.
func (s *BaseKsqlGrammarListener) ExitListStreams(ctx *ListStreamsContext) {}

// EnterListTables is called when production listTables is entered.
func (s *BaseKsqlGrammarListener) EnterListTables(ctx *ListTablesContext) {}

// ExitListTables is called when production listTables is exited.
func (s *BaseKsqlGrammarListener) ExitListTables(ctx *ListTablesContext) {}

// EnterListFunctions is called when production listFunctions is entered.
func (s *BaseKsqlGrammarListener) EnterListFunctions(ctx *ListFunctionsContext) {}

// ExitListFunctions is called when production listFunctions is exited.
func (s *BaseKsqlGrammarListener) ExitListFunctions(ctx *ListFunctionsContext) {}

// EnterListConnectors is called when production listConnectors is entered.
func (s *BaseKsqlGrammarListener) EnterListConnectors(ctx *ListConnectorsContext) {}

// ExitListConnectors is called when production listConnectors is exited.
func (s *BaseKsqlGrammarListener) ExitListConnectors(ctx *ListConnectorsContext) {}

// EnterListConnectorPlugins is called when production listConnectorPlugins is entered.
func (s *BaseKsqlGrammarListener) EnterListConnectorPlugins(ctx *ListConnectorPluginsContext) {}

// ExitListConnectorPlugins is called when production listConnectorPlugins is exited.
func (s *BaseKsqlGrammarListener) ExitListConnectorPlugins(ctx *ListConnectorPluginsContext) {}

// EnterListTypes is called when production listTypes is entered.
func (s *BaseKsqlGrammarListener) EnterListTypes(ctx *ListTypesContext) {}

// ExitListTypes is called when production listTypes is exited.
func (s *BaseKsqlGrammarListener) ExitListTypes(ctx *ListTypesContext) {}

// EnterListVariables is called when production listVariables is entered.
func (s *BaseKsqlGrammarListener) EnterListVariables(ctx *ListVariablesContext) {}

// ExitListVariables is called when production listVariables is exited.
func (s *BaseKsqlGrammarListener) ExitListVariables(ctx *ListVariablesContext) {}

// EnterShowColumns is called when production showColumns is entered.
func (s *BaseKsqlGrammarListener) EnterShowColumns(ctx *ShowColumnsContext) {}

// ExitShowColumns is called when production showColumns is exited.
func (s *BaseKsqlGrammarListener) ExitShowColumns(ctx *ShowColumnsContext) {}

// EnterDescribeStreams is called when production describeStreams is entered.
func (s *BaseKsqlGrammarListener) EnterDescribeStreams(ctx *DescribeStreamsContext) {}

// ExitDescribeStreams is called when production describeStreams is exited.
func (s *BaseKsqlGrammarListener) ExitDescribeStreams(ctx *DescribeStreamsContext) {}

// EnterDescribeFunction is called when production describeFunction is entered.
func (s *BaseKsqlGrammarListener) EnterDescribeFunction(ctx *DescribeFunctionContext) {}

// ExitDescribeFunction is called when production describeFunction is exited.
func (s *BaseKsqlGrammarListener) ExitDescribeFunction(ctx *DescribeFunctionContext) {}

// EnterDescribeConnector is called when production describeConnector is entered.
func (s *BaseKsqlGrammarListener) EnterDescribeConnector(ctx *DescribeConnectorContext) {}

// ExitDescribeConnector is called when production describeConnector is exited.
func (s *BaseKsqlGrammarListener) ExitDescribeConnector(ctx *DescribeConnectorContext) {}

// EnterPrintTopic is called when production printTopic is entered.
func (s *BaseKsqlGrammarListener) EnterPrintTopic(ctx *PrintTopicContext) {}

// ExitPrintTopic is called when production printTopic is exited.
func (s *BaseKsqlGrammarListener) ExitPrintTopic(ctx *PrintTopicContext) {}

// EnterListQueries is called when production listQueries is entered.
func (s *BaseKsqlGrammarListener) EnterListQueries(ctx *ListQueriesContext) {}

// ExitListQueries is called when production listQueries is exited.
func (s *BaseKsqlGrammarListener) ExitListQueries(ctx *ListQueriesContext) {}

// EnterTerminateQuery is called when production terminateQuery is entered.
func (s *BaseKsqlGrammarListener) EnterTerminateQuery(ctx *TerminateQueryContext) {}

// ExitTerminateQuery is called when production terminateQuery is exited.
func (s *BaseKsqlGrammarListener) ExitTerminateQuery(ctx *TerminateQueryContext) {}

// EnterSetProperty is called when production setProperty is entered.
func (s *BaseKsqlGrammarListener) EnterSetProperty(ctx *SetPropertyContext) {}

// ExitSetProperty is called when production setProperty is exited.
func (s *BaseKsqlGrammarListener) ExitSetProperty(ctx *SetPropertyContext) {}

// EnterUnsetProperty is called when production unsetProperty is entered.
func (s *BaseKsqlGrammarListener) EnterUnsetProperty(ctx *UnsetPropertyContext) {}

// ExitUnsetProperty is called when production unsetProperty is exited.
func (s *BaseKsqlGrammarListener) ExitUnsetProperty(ctx *UnsetPropertyContext) {}

// EnterDefineVariable is called when production defineVariable is entered.
func (s *BaseKsqlGrammarListener) EnterDefineVariable(ctx *DefineVariableContext) {}

// ExitDefineVariable is called when production defineVariable is exited.
func (s *BaseKsqlGrammarListener) ExitDefineVariable(ctx *DefineVariableContext) {}

// EnterUndefineVariable is called when production undefineVariable is entered.
func (s *BaseKsqlGrammarListener) EnterUndefineVariable(ctx *UndefineVariableContext) {}

// ExitUndefineVariable is called when production undefineVariable is exited.
func (s *BaseKsqlGrammarListener) ExitUndefineVariable(ctx *UndefineVariableContext) {}

// EnterCreateStream is called when production createStream is entered.
func (s *BaseKsqlGrammarListener) EnterCreateStream(ctx *CreateStreamContext) {}

// ExitCreateStream is called when production createStream is exited.
func (s *BaseKsqlGrammarListener) ExitCreateStream(ctx *CreateStreamContext) {}

// EnterCreateStreamAs is called when production createStreamAs is entered.
func (s *BaseKsqlGrammarListener) EnterCreateStreamAs(ctx *CreateStreamAsContext) {}

// ExitCreateStreamAs is called when production createStreamAs is exited.
func (s *BaseKsqlGrammarListener) ExitCreateStreamAs(ctx *CreateStreamAsContext) {}

// EnterCreateTable is called when production createTable is entered.
func (s *BaseKsqlGrammarListener) EnterCreateTable(ctx *CreateTableContext) {}

// ExitCreateTable is called when production createTable is exited.
func (s *BaseKsqlGrammarListener) ExitCreateTable(ctx *CreateTableContext) {}

// EnterCreateTableAs is called when production createTableAs is entered.
func (s *BaseKsqlGrammarListener) EnterCreateTableAs(ctx *CreateTableAsContext) {}

// ExitCreateTableAs is called when production createTableAs is exited.
func (s *BaseKsqlGrammarListener) ExitCreateTableAs(ctx *CreateTableAsContext) {}

// EnterCreateConnector is called when production createConnector is entered.
func (s *BaseKsqlGrammarListener) EnterCreateConnector(ctx *CreateConnectorContext) {}

// ExitCreateConnector is called when production createConnector is exited.
func (s *BaseKsqlGrammarListener) ExitCreateConnector(ctx *CreateConnectorContext) {}

// EnterInsertInto is called when production insertInto is entered.
func (s *BaseKsqlGrammarListener) EnterInsertInto(ctx *InsertIntoContext) {}

// ExitInsertInto is called when production insertInto is exited.
func (s *BaseKsqlGrammarListener) ExitInsertInto(ctx *InsertIntoContext) {}

// EnterInsertValues is called when production insertValues is entered.
func (s *BaseKsqlGrammarListener) EnterInsertValues(ctx *InsertValuesContext) {}

// ExitInsertValues is called when production insertValues is exited.
func (s *BaseKsqlGrammarListener) ExitInsertValues(ctx *InsertValuesContext) {}

// EnterDropStream is called when production dropStream is entered.
func (s *BaseKsqlGrammarListener) EnterDropStream(ctx *DropStreamContext) {}

// ExitDropStream is called when production dropStream is exited.
func (s *BaseKsqlGrammarListener) ExitDropStream(ctx *DropStreamContext) {}

// EnterDropTable is called when production dropTable is entered.
func (s *BaseKsqlGrammarListener) EnterDropTable(ctx *DropTableContext) {}

// ExitDropTable is called when production dropTable is exited.
func (s *BaseKsqlGrammarListener) ExitDropTable(ctx *DropTableContext) {}

// EnterDropConnector is called when production dropConnector is entered.
func (s *BaseKsqlGrammarListener) EnterDropConnector(ctx *DropConnectorContext) {}

// ExitDropConnector is called when production dropConnector is exited.
func (s *BaseKsqlGrammarListener) ExitDropConnector(ctx *DropConnectorContext) {}

// EnterExplain is called when production explain is entered.
func (s *BaseKsqlGrammarListener) EnterExplain(ctx *ExplainContext) {}

// ExitExplain is called when production explain is exited.
func (s *BaseKsqlGrammarListener) ExitExplain(ctx *ExplainContext) {}

// EnterRegisterType is called when production registerType is entered.
func (s *BaseKsqlGrammarListener) EnterRegisterType(ctx *RegisterTypeContext) {}

// ExitRegisterType is called when production registerType is exited.
func (s *BaseKsqlGrammarListener) ExitRegisterType(ctx *RegisterTypeContext) {}

// EnterDropType is called when production dropType is entered.
func (s *BaseKsqlGrammarListener) EnterDropType(ctx *DropTypeContext) {}

// ExitDropType is called when production dropType is exited.
func (s *BaseKsqlGrammarListener) ExitDropType(ctx *DropTypeContext) {}

// EnterAlterSource is called when production alterSource is entered.
func (s *BaseKsqlGrammarListener) EnterAlterSource(ctx *AlterSourceContext) {}

// ExitAlterSource is called when production alterSource is exited.
func (s *BaseKsqlGrammarListener) ExitAlterSource(ctx *AlterSourceContext) {}

// EnterAssertValues is called when production assertValues is entered.
func (s *BaseKsqlGrammarListener) EnterAssertValues(ctx *AssertValuesContext) {}

// ExitAssertValues is called when production assertValues is exited.
func (s *BaseKsqlGrammarListener) ExitAssertValues(ctx *AssertValuesContext) {}

// EnterAssertTombstone is called when production assertTombstone is entered.
func (s *BaseKsqlGrammarListener) EnterAssertTombstone(ctx *AssertTombstoneContext) {}

// ExitAssertTombstone is called when production assertTombstone is exited.
func (s *BaseKsqlGrammarListener) ExitAssertTombstone(ctx *AssertTombstoneContext) {}

// EnterAssertStream is called when production assertStream is entered.
func (s *BaseKsqlGrammarListener) EnterAssertStream(ctx *AssertStreamContext) {}

// ExitAssertStream is called when production assertStream is exited.
func (s *BaseKsqlGrammarListener) ExitAssertStream(ctx *AssertStreamContext) {}

// EnterAssertTable is called when production assertTable is entered.
func (s *BaseKsqlGrammarListener) EnterAssertTable(ctx *AssertTableContext) {}

// ExitAssertTable is called when production assertTable is exited.
func (s *BaseKsqlGrammarListener) ExitAssertTable(ctx *AssertTableContext) {}

// EnterRunScript is called when production runScript is entered.
func (s *BaseKsqlGrammarListener) EnterRunScript(ctx *RunScriptContext) {}

// ExitRunScript is called when production runScript is exited.
func (s *BaseKsqlGrammarListener) ExitRunScript(ctx *RunScriptContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseKsqlGrammarListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseKsqlGrammarListener) ExitQuery(ctx *QueryContext) {}

// EnterResultMaterialization is called when production resultMaterialization is entered.
func (s *BaseKsqlGrammarListener) EnterResultMaterialization(ctx *ResultMaterializationContext) {}

// ExitResultMaterialization is called when production resultMaterialization is exited.
func (s *BaseKsqlGrammarListener) ExitResultMaterialization(ctx *ResultMaterializationContext) {}

// EnterAlterOption is called when production alterOption is entered.
func (s *BaseKsqlGrammarListener) EnterAlterOption(ctx *AlterOptionContext) {}

// ExitAlterOption is called when production alterOption is exited.
func (s *BaseKsqlGrammarListener) ExitAlterOption(ctx *AlterOptionContext) {}

// EnterTableElements is called when production tableElements is entered.
func (s *BaseKsqlGrammarListener) EnterTableElements(ctx *TableElementsContext) {}

// ExitTableElements is called when production tableElements is exited.
func (s *BaseKsqlGrammarListener) ExitTableElements(ctx *TableElementsContext) {}

// EnterTableElement is called when production tableElement is entered.
func (s *BaseKsqlGrammarListener) EnterTableElement(ctx *TableElementContext) {}

// ExitTableElement is called when production tableElement is exited.
func (s *BaseKsqlGrammarListener) ExitTableElement(ctx *TableElementContext) {}

// EnterColumnConstraints is called when production columnConstraints is entered.
func (s *BaseKsqlGrammarListener) EnterColumnConstraints(ctx *ColumnConstraintsContext) {}

// ExitColumnConstraints is called when production columnConstraints is exited.
func (s *BaseKsqlGrammarListener) ExitColumnConstraints(ctx *ColumnConstraintsContext) {}

// EnterTableProperties is called when production tableProperties is entered.
func (s *BaseKsqlGrammarListener) EnterTableProperties(ctx *TablePropertiesContext) {}

// ExitTableProperties is called when production tableProperties is exited.
func (s *BaseKsqlGrammarListener) ExitTableProperties(ctx *TablePropertiesContext) {}

// EnterTableProperty is called when production tableProperty is entered.
func (s *BaseKsqlGrammarListener) EnterTableProperty(ctx *TablePropertyContext) {}

// ExitTableProperty is called when production tableProperty is exited.
func (s *BaseKsqlGrammarListener) ExitTableProperty(ctx *TablePropertyContext) {}

// EnterPrintClause is called when production printClause is entered.
func (s *BaseKsqlGrammarListener) EnterPrintClause(ctx *PrintClauseContext) {}

// ExitPrintClause is called when production printClause is exited.
func (s *BaseKsqlGrammarListener) ExitPrintClause(ctx *PrintClauseContext) {}

// EnterIntervalClause is called when production intervalClause is entered.
func (s *BaseKsqlGrammarListener) EnterIntervalClause(ctx *IntervalClauseContext) {}

// ExitIntervalClause is called when production intervalClause is exited.
func (s *BaseKsqlGrammarListener) ExitIntervalClause(ctx *IntervalClauseContext) {}

// EnterLimitClause is called when production limitClause is entered.
func (s *BaseKsqlGrammarListener) EnterLimitClause(ctx *LimitClauseContext) {}

// ExitLimitClause is called when production limitClause is exited.
func (s *BaseKsqlGrammarListener) ExitLimitClause(ctx *LimitClauseContext) {}

// EnterRetentionClause is called when production retentionClause is entered.
func (s *BaseKsqlGrammarListener) EnterRetentionClause(ctx *RetentionClauseContext) {}

// ExitRetentionClause is called when production retentionClause is exited.
func (s *BaseKsqlGrammarListener) ExitRetentionClause(ctx *RetentionClauseContext) {}

// EnterGracePeriodClause is called when production gracePeriodClause is entered.
func (s *BaseKsqlGrammarListener) EnterGracePeriodClause(ctx *GracePeriodClauseContext) {}

// ExitGracePeriodClause is called when production gracePeriodClause is exited.
func (s *BaseKsqlGrammarListener) ExitGracePeriodClause(ctx *GracePeriodClauseContext) {}

// EnterWindowExpression is called when production windowExpression is entered.
func (s *BaseKsqlGrammarListener) EnterWindowExpression(ctx *WindowExpressionContext) {}

// ExitWindowExpression is called when production windowExpression is exited.
func (s *BaseKsqlGrammarListener) ExitWindowExpression(ctx *WindowExpressionContext) {}

// EnterTumblingWindowExpression is called when production tumblingWindowExpression is entered.
func (s *BaseKsqlGrammarListener) EnterTumblingWindowExpression(ctx *TumblingWindowExpressionContext) {
}

// ExitTumblingWindowExpression is called when production tumblingWindowExpression is exited.
func (s *BaseKsqlGrammarListener) ExitTumblingWindowExpression(ctx *TumblingWindowExpressionContext) {
}

// EnterHoppingWindowExpression is called when production hoppingWindowExpression is entered.
func (s *BaseKsqlGrammarListener) EnterHoppingWindowExpression(ctx *HoppingWindowExpressionContext) {}

// ExitHoppingWindowExpression is called when production hoppingWindowExpression is exited.
func (s *BaseKsqlGrammarListener) ExitHoppingWindowExpression(ctx *HoppingWindowExpressionContext) {}

// EnterSessionWindowExpression is called when production sessionWindowExpression is entered.
func (s *BaseKsqlGrammarListener) EnterSessionWindowExpression(ctx *SessionWindowExpressionContext) {}

// ExitSessionWindowExpression is called when production sessionWindowExpression is exited.
func (s *BaseKsqlGrammarListener) ExitSessionWindowExpression(ctx *SessionWindowExpressionContext) {}

// EnterWindowUnit is called when production windowUnit is entered.
func (s *BaseKsqlGrammarListener) EnterWindowUnit(ctx *WindowUnitContext) {}

// ExitWindowUnit is called when production windowUnit is exited.
func (s *BaseKsqlGrammarListener) ExitWindowUnit(ctx *WindowUnitContext) {}

// EnterGroupBy is called when production groupBy is entered.
func (s *BaseKsqlGrammarListener) EnterGroupBy(ctx *GroupByContext) {}

// ExitGroupBy is called when production groupBy is exited.
func (s *BaseKsqlGrammarListener) ExitGroupBy(ctx *GroupByContext) {}

// EnterPartitionBy is called when production partitionBy is entered.
func (s *BaseKsqlGrammarListener) EnterPartitionBy(ctx *PartitionByContext) {}

// ExitPartitionBy is called when production partitionBy is exited.
func (s *BaseKsqlGrammarListener) ExitPartitionBy(ctx *PartitionByContext) {}

// EnterValues is called when production values is entered.
func (s *BaseKsqlGrammarListener) EnterValues(ctx *ValuesContext) {}

// ExitValues is called when production values is exited.
func (s *BaseKsqlGrammarListener) ExitValues(ctx *ValuesContext) {}

// EnterSelectSingle is called when production selectSingle is entered.
func (s *BaseKsqlGrammarListener) EnterSelectSingle(ctx *SelectSingleContext) {}

// ExitSelectSingle is called when production selectSingle is exited.
func (s *BaseKsqlGrammarListener) ExitSelectSingle(ctx *SelectSingleContext) {}

// EnterSelectAll is called when production selectAll is entered.
func (s *BaseKsqlGrammarListener) EnterSelectAll(ctx *SelectAllContext) {}

// ExitSelectAll is called when production selectAll is exited.
func (s *BaseKsqlGrammarListener) ExitSelectAll(ctx *SelectAllContext) {}

// EnterJoinRelation is called when production joinRelation is entered.
func (s *BaseKsqlGrammarListener) EnterJoinRelation(ctx *JoinRelationContext) {}

// ExitJoinRelation is called when production joinRelation is exited.
func (s *BaseKsqlGrammarListener) ExitJoinRelation(ctx *JoinRelationContext) {}

// EnterRelationDefault is called when production relationDefault is entered.
func (s *BaseKsqlGrammarListener) EnterRelationDefault(ctx *RelationDefaultContext) {}

// ExitRelationDefault is called when production relationDefault is exited.
func (s *BaseKsqlGrammarListener) ExitRelationDefault(ctx *RelationDefaultContext) {}

// EnterJoinedSource is called when production joinedSource is entered.
func (s *BaseKsqlGrammarListener) EnterJoinedSource(ctx *JoinedSourceContext) {}

// ExitJoinedSource is called when production joinedSource is exited.
func (s *BaseKsqlGrammarListener) ExitJoinedSource(ctx *JoinedSourceContext) {}

// EnterInnerJoin is called when production innerJoin is entered.
func (s *BaseKsqlGrammarListener) EnterInnerJoin(ctx *InnerJoinContext) {}

// ExitInnerJoin is called when production innerJoin is exited.
func (s *BaseKsqlGrammarListener) ExitInnerJoin(ctx *InnerJoinContext) {}

// EnterOuterJoin is called when production outerJoin is entered.
func (s *BaseKsqlGrammarListener) EnterOuterJoin(ctx *OuterJoinContext) {}

// ExitOuterJoin is called when production outerJoin is exited.
func (s *BaseKsqlGrammarListener) ExitOuterJoin(ctx *OuterJoinContext) {}

// EnterLeftJoin is called when production leftJoin is entered.
func (s *BaseKsqlGrammarListener) EnterLeftJoin(ctx *LeftJoinContext) {}

// ExitLeftJoin is called when production leftJoin is exited.
func (s *BaseKsqlGrammarListener) ExitLeftJoin(ctx *LeftJoinContext) {}

// EnterJoinWindow is called when production joinWindow is entered.
func (s *BaseKsqlGrammarListener) EnterJoinWindow(ctx *JoinWindowContext) {}

// ExitJoinWindow is called when production joinWindow is exited.
func (s *BaseKsqlGrammarListener) ExitJoinWindow(ctx *JoinWindowContext) {}

// EnterJoinWindowWithBeforeAndAfter is called when production joinWindowWithBeforeAndAfter is entered.
func (s *BaseKsqlGrammarListener) EnterJoinWindowWithBeforeAndAfter(ctx *JoinWindowWithBeforeAndAfterContext) {
}

// ExitJoinWindowWithBeforeAndAfter is called when production joinWindowWithBeforeAndAfter is exited.
func (s *BaseKsqlGrammarListener) ExitJoinWindowWithBeforeAndAfter(ctx *JoinWindowWithBeforeAndAfterContext) {
}

// EnterSingleJoinWindow is called when production singleJoinWindow is entered.
func (s *BaseKsqlGrammarListener) EnterSingleJoinWindow(ctx *SingleJoinWindowContext) {}

// ExitSingleJoinWindow is called when production singleJoinWindow is exited.
func (s *BaseKsqlGrammarListener) ExitSingleJoinWindow(ctx *SingleJoinWindowContext) {}

// EnterJoinWindowSize is called when production joinWindowSize is entered.
func (s *BaseKsqlGrammarListener) EnterJoinWindowSize(ctx *JoinWindowSizeContext) {}

// ExitJoinWindowSize is called when production joinWindowSize is exited.
func (s *BaseKsqlGrammarListener) ExitJoinWindowSize(ctx *JoinWindowSizeContext) {}

// EnterJoinCriteria is called when production joinCriteria is entered.
func (s *BaseKsqlGrammarListener) EnterJoinCriteria(ctx *JoinCriteriaContext) {}

// ExitJoinCriteria is called when production joinCriteria is exited.
func (s *BaseKsqlGrammarListener) ExitJoinCriteria(ctx *JoinCriteriaContext) {}

// EnterAliasedRelation is called when production aliasedRelation is entered.
func (s *BaseKsqlGrammarListener) EnterAliasedRelation(ctx *AliasedRelationContext) {}

// ExitAliasedRelation is called when production aliasedRelation is exited.
func (s *BaseKsqlGrammarListener) ExitAliasedRelation(ctx *AliasedRelationContext) {}

// EnterColumns is called when production columns is entered.
func (s *BaseKsqlGrammarListener) EnterColumns(ctx *ColumnsContext) {}

// ExitColumns is called when production columns is exited.
func (s *BaseKsqlGrammarListener) ExitColumns(ctx *ColumnsContext) {}

// EnterTableName is called when production tableName is entered.
func (s *BaseKsqlGrammarListener) EnterTableName(ctx *TableNameContext) {}

// ExitTableName is called when production tableName is exited.
func (s *BaseKsqlGrammarListener) ExitTableName(ctx *TableNameContext) {}

// EnterExpression is called when production expression is entered.
func (s *BaseKsqlGrammarListener) EnterExpression(ctx *ExpressionContext) {}

// ExitExpression is called when production expression is exited.
func (s *BaseKsqlGrammarListener) ExitExpression(ctx *ExpressionContext) {}

// EnterLogicalNot is called when production logicalNot is entered.
func (s *BaseKsqlGrammarListener) EnterLogicalNot(ctx *LogicalNotContext) {}

// ExitLogicalNot is called when production logicalNot is exited.
func (s *BaseKsqlGrammarListener) ExitLogicalNot(ctx *LogicalNotContext) {}

// EnterBooleanDefault is called when production booleanDefault is entered.
func (s *BaseKsqlGrammarListener) EnterBooleanDefault(ctx *BooleanDefaultContext) {}

// ExitBooleanDefault is called when production booleanDefault is exited.
func (s *BaseKsqlGrammarListener) ExitBooleanDefault(ctx *BooleanDefaultContext) {}

// EnterLogicalBinary is called when production logicalBinary is entered.
func (s *BaseKsqlGrammarListener) EnterLogicalBinary(ctx *LogicalBinaryContext) {}

// ExitLogicalBinary is called when production logicalBinary is exited.
func (s *BaseKsqlGrammarListener) ExitLogicalBinary(ctx *LogicalBinaryContext) {}

// EnterPredicated is called when production predicated is entered.
func (s *BaseKsqlGrammarListener) EnterPredicated(ctx *PredicatedContext) {}

// ExitPredicated is called when production predicated is exited.
func (s *BaseKsqlGrammarListener) ExitPredicated(ctx *PredicatedContext) {}

// EnterComparison is called when production comparison is entered.
func (s *BaseKsqlGrammarListener) EnterComparison(ctx *ComparisonContext) {}

// ExitComparison is called when production comparison is exited.
func (s *BaseKsqlGrammarListener) ExitComparison(ctx *ComparisonContext) {}

// EnterBetween is called when production between is entered.
func (s *BaseKsqlGrammarListener) EnterBetween(ctx *BetweenContext) {}

// ExitBetween is called when production between is exited.
func (s *BaseKsqlGrammarListener) ExitBetween(ctx *BetweenContext) {}

// EnterInList is called when production inList is entered.
func (s *BaseKsqlGrammarListener) EnterInList(ctx *InListContext) {}

// ExitInList is called when production inList is exited.
func (s *BaseKsqlGrammarListener) ExitInList(ctx *InListContext) {}

// EnterLike is called when production like is entered.
func (s *BaseKsqlGrammarListener) EnterLike(ctx *LikeContext) {}

// ExitLike is called when production like is exited.
func (s *BaseKsqlGrammarListener) ExitLike(ctx *LikeContext) {}

// EnterNullPredicate is called when production nullPredicate is entered.
func (s *BaseKsqlGrammarListener) EnterNullPredicate(ctx *NullPredicateContext) {}

// ExitNullPredicate is called when production nullPredicate is exited.
func (s *BaseKsqlGrammarListener) ExitNullPredicate(ctx *NullPredicateContext) {}

// EnterDistinctFrom is called when production distinctFrom is entered.
func (s *BaseKsqlGrammarListener) EnterDistinctFrom(ctx *DistinctFromContext) {}

// ExitDistinctFrom is called when production distinctFrom is exited.
func (s *BaseKsqlGrammarListener) ExitDistinctFrom(ctx *DistinctFromContext) {}

// EnterValueExpressionDefault is called when production valueExpressionDefault is entered.
func (s *BaseKsqlGrammarListener) EnterValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// ExitValueExpressionDefault is called when production valueExpressionDefault is exited.
func (s *BaseKsqlGrammarListener) ExitValueExpressionDefault(ctx *ValueExpressionDefaultContext) {}

// EnterConcatenation is called when production concatenation is entered.
func (s *BaseKsqlGrammarListener) EnterConcatenation(ctx *ConcatenationContext) {}

// ExitConcatenation is called when production concatenation is exited.
func (s *BaseKsqlGrammarListener) ExitConcatenation(ctx *ConcatenationContext) {}

// EnterArithmeticBinary is called when production arithmeticBinary is entered.
func (s *BaseKsqlGrammarListener) EnterArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// ExitArithmeticBinary is called when production arithmeticBinary is exited.
func (s *BaseKsqlGrammarListener) ExitArithmeticBinary(ctx *ArithmeticBinaryContext) {}

// EnterArithmeticUnary is called when production arithmeticUnary is entered.
func (s *BaseKsqlGrammarListener) EnterArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// ExitArithmeticUnary is called when production arithmeticUnary is exited.
func (s *BaseKsqlGrammarListener) ExitArithmeticUnary(ctx *ArithmeticUnaryContext) {}

// EnterAtTimeZone is called when production atTimeZone is entered.
func (s *BaseKsqlGrammarListener) EnterAtTimeZone(ctx *AtTimeZoneContext) {}

// ExitAtTimeZone is called when production atTimeZone is exited.
func (s *BaseKsqlGrammarListener) ExitAtTimeZone(ctx *AtTimeZoneContext) {}

// EnterDereference is called when production dereference is entered.
func (s *BaseKsqlGrammarListener) EnterDereference(ctx *DereferenceContext) {}

// ExitDereference is called when production dereference is exited.
func (s *BaseKsqlGrammarListener) ExitDereference(ctx *DereferenceContext) {}

// EnterSimpleCase is called when production simpleCase is entered.
func (s *BaseKsqlGrammarListener) EnterSimpleCase(ctx *SimpleCaseContext) {}

// ExitSimpleCase is called when production simpleCase is exited.
func (s *BaseKsqlGrammarListener) ExitSimpleCase(ctx *SimpleCaseContext) {}

// EnterColumnReference is called when production columnReference is entered.
func (s *BaseKsqlGrammarListener) EnterColumnReference(ctx *ColumnReferenceContext) {}

// ExitColumnReference is called when production columnReference is exited.
func (s *BaseKsqlGrammarListener) ExitColumnReference(ctx *ColumnReferenceContext) {}

// EnterSubscript is called when production subscript is entered.
func (s *BaseKsqlGrammarListener) EnterSubscript(ctx *SubscriptContext) {}

// ExitSubscript is called when production subscript is exited.
func (s *BaseKsqlGrammarListener) ExitSubscript(ctx *SubscriptContext) {}

// EnterStructConstructor is called when production structConstructor is entered.
func (s *BaseKsqlGrammarListener) EnterStructConstructor(ctx *StructConstructorContext) {}

// ExitStructConstructor is called when production structConstructor is exited.
func (s *BaseKsqlGrammarListener) ExitStructConstructor(ctx *StructConstructorContext) {}

// EnterTypeConstructor is called when production typeConstructor is entered.
func (s *BaseKsqlGrammarListener) EnterTypeConstructor(ctx *TypeConstructorContext) {}

// ExitTypeConstructor is called when production typeConstructor is exited.
func (s *BaseKsqlGrammarListener) ExitTypeConstructor(ctx *TypeConstructorContext) {}

// EnterQualifiedColumnReference is called when production qualifiedColumnReference is entered.
func (s *BaseKsqlGrammarListener) EnterQualifiedColumnReference(ctx *QualifiedColumnReferenceContext) {
}

// ExitQualifiedColumnReference is called when production qualifiedColumnReference is exited.
func (s *BaseKsqlGrammarListener) ExitQualifiedColumnReference(ctx *QualifiedColumnReferenceContext) {
}

// EnterCast is called when production cast is entered.
func (s *BaseKsqlGrammarListener) EnterCast(ctx *CastContext) {}

// ExitCast is called when production cast is exited.
func (s *BaseKsqlGrammarListener) ExitCast(ctx *CastContext) {}

// EnterParenthesizedExpression is called when production parenthesizedExpression is entered.
func (s *BaseKsqlGrammarListener) EnterParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// ExitParenthesizedExpression is called when production parenthesizedExpression is exited.
func (s *BaseKsqlGrammarListener) ExitParenthesizedExpression(ctx *ParenthesizedExpressionContext) {}

// EnterArrayConstructor is called when production arrayConstructor is entered.
func (s *BaseKsqlGrammarListener) EnterArrayConstructor(ctx *ArrayConstructorContext) {}

// ExitArrayConstructor is called when production arrayConstructor is exited.
func (s *BaseKsqlGrammarListener) ExitArrayConstructor(ctx *ArrayConstructorContext) {}

// EnterMapConstructor is called when production mapConstructor is entered.
func (s *BaseKsqlGrammarListener) EnterMapConstructor(ctx *MapConstructorContext) {}

// ExitMapConstructor is called when production mapConstructor is exited.
func (s *BaseKsqlGrammarListener) ExitMapConstructor(ctx *MapConstructorContext) {}

// EnterFunctionCall is called when production functionCall is entered.
func (s *BaseKsqlGrammarListener) EnterFunctionCall(ctx *FunctionCallContext) {}

// ExitFunctionCall is called when production functionCall is exited.
func (s *BaseKsqlGrammarListener) ExitFunctionCall(ctx *FunctionCallContext) {}

// EnterSearchedCase is called when production searchedCase is entered.
func (s *BaseKsqlGrammarListener) EnterSearchedCase(ctx *SearchedCaseContext) {}

// ExitSearchedCase is called when production searchedCase is exited.
func (s *BaseKsqlGrammarListener) ExitSearchedCase(ctx *SearchedCaseContext) {}

// EnterLiteralExpression is called when production literalExpression is entered.
func (s *BaseKsqlGrammarListener) EnterLiteralExpression(ctx *LiteralExpressionContext) {}

// ExitLiteralExpression is called when production literalExpression is exited.
func (s *BaseKsqlGrammarListener) ExitLiteralExpression(ctx *LiteralExpressionContext) {}

// EnterFunctionArgument is called when production functionArgument is entered.
func (s *BaseKsqlGrammarListener) EnterFunctionArgument(ctx *FunctionArgumentContext) {}

// ExitFunctionArgument is called when production functionArgument is exited.
func (s *BaseKsqlGrammarListener) ExitFunctionArgument(ctx *FunctionArgumentContext) {}

// EnterTimeZoneString is called when production timeZoneString is entered.
func (s *BaseKsqlGrammarListener) EnterTimeZoneString(ctx *TimeZoneStringContext) {}

// ExitTimeZoneString is called when production timeZoneString is exited.
func (s *BaseKsqlGrammarListener) ExitTimeZoneString(ctx *TimeZoneStringContext) {}

// EnterComparisonOperator is called when production comparisonOperator is entered.
func (s *BaseKsqlGrammarListener) EnterComparisonOperator(ctx *ComparisonOperatorContext) {}

// ExitComparisonOperator is called when production comparisonOperator is exited.
func (s *BaseKsqlGrammarListener) ExitComparisonOperator(ctx *ComparisonOperatorContext) {}

// EnterBooleanValue is called when production booleanValue is entered.
func (s *BaseKsqlGrammarListener) EnterBooleanValue(ctx *BooleanValueContext) {}

// ExitBooleanValue is called when production booleanValue is exited.
func (s *BaseKsqlGrammarListener) ExitBooleanValue(ctx *BooleanValueContext) {}

// EnterType is called when production type is entered.
func (s *BaseKsqlGrammarListener) EnterType(ctx *TypeContext) {}

// ExitType is called when production type is exited.
func (s *BaseKsqlGrammarListener) ExitType(ctx *TypeContext) {}

// EnterTypeParameter is called when production typeParameter is entered.
func (s *BaseKsqlGrammarListener) EnterTypeParameter(ctx *TypeParameterContext) {}

// ExitTypeParameter is called when production typeParameter is exited.
func (s *BaseKsqlGrammarListener) ExitTypeParameter(ctx *TypeParameterContext) {}

// EnterBaseType is called when production baseType is entered.
func (s *BaseKsqlGrammarListener) EnterBaseType(ctx *BaseTypeContext) {}

// ExitBaseType is called when production baseType is exited.
func (s *BaseKsqlGrammarListener) ExitBaseType(ctx *BaseTypeContext) {}

// EnterWhenClause is called when production whenClause is entered.
func (s *BaseKsqlGrammarListener) EnterWhenClause(ctx *WhenClauseContext) {}

// ExitWhenClause is called when production whenClause is exited.
func (s *BaseKsqlGrammarListener) ExitWhenClause(ctx *WhenClauseContext) {}

// EnterVariableIdentifier is called when production variableIdentifier is entered.
func (s *BaseKsqlGrammarListener) EnterVariableIdentifier(ctx *VariableIdentifierContext) {}

// ExitVariableIdentifier is called when production variableIdentifier is exited.
func (s *BaseKsqlGrammarListener) ExitVariableIdentifier(ctx *VariableIdentifierContext) {}

// EnterUnquotedIdentifier is called when production unquotedIdentifier is entered.
func (s *BaseKsqlGrammarListener) EnterUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// ExitUnquotedIdentifier is called when production unquotedIdentifier is exited.
func (s *BaseKsqlGrammarListener) ExitUnquotedIdentifier(ctx *UnquotedIdentifierContext) {}

// EnterQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is entered.
func (s *BaseKsqlGrammarListener) EnterQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// ExitQuotedIdentifierAlternative is called when production quotedIdentifierAlternative is exited.
func (s *BaseKsqlGrammarListener) ExitQuotedIdentifierAlternative(ctx *QuotedIdentifierAlternativeContext) {
}

// EnterBackQuotedIdentifier is called when production backQuotedIdentifier is entered.
func (s *BaseKsqlGrammarListener) EnterBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// ExitBackQuotedIdentifier is called when production backQuotedIdentifier is exited.
func (s *BaseKsqlGrammarListener) ExitBackQuotedIdentifier(ctx *BackQuotedIdentifierContext) {}

// EnterDigitIdentifier is called when production digitIdentifier is entered.
func (s *BaseKsqlGrammarListener) EnterDigitIdentifier(ctx *DigitIdentifierContext) {}

// ExitDigitIdentifier is called when production digitIdentifier is exited.
func (s *BaseKsqlGrammarListener) ExitDigitIdentifier(ctx *DigitIdentifierContext) {}

// EnterLambda is called when production lambda is entered.
func (s *BaseKsqlGrammarListener) EnterLambda(ctx *LambdaContext) {}

// ExitLambda is called when production lambda is exited.
func (s *BaseKsqlGrammarListener) ExitLambda(ctx *LambdaContext) {}

// EnterVariableName is called when production variableName is entered.
func (s *BaseKsqlGrammarListener) EnterVariableName(ctx *VariableNameContext) {}

// ExitVariableName is called when production variableName is exited.
func (s *BaseKsqlGrammarListener) ExitVariableName(ctx *VariableNameContext) {}

// EnterVariableValue is called when production variableValue is entered.
func (s *BaseKsqlGrammarListener) EnterVariableValue(ctx *VariableValueContext) {}

// ExitVariableValue is called when production variableValue is exited.
func (s *BaseKsqlGrammarListener) ExitVariableValue(ctx *VariableValueContext) {}

// EnterSourceName is called when production sourceName is entered.
func (s *BaseKsqlGrammarListener) EnterSourceName(ctx *SourceNameContext) {}

// ExitSourceName is called when production sourceName is exited.
func (s *BaseKsqlGrammarListener) ExitSourceName(ctx *SourceNameContext) {}

// EnterDecimalLiteral is called when production decimalLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterDecimalLiteral(ctx *DecimalLiteralContext) {}

// ExitDecimalLiteral is called when production decimalLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitDecimalLiteral(ctx *DecimalLiteralContext) {}

// EnterFloatLiteral is called when production floatLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterFloatLiteral(ctx *FloatLiteralContext) {}

// ExitFloatLiteral is called when production floatLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitFloatLiteral(ctx *FloatLiteralContext) {}

// EnterIntegerLiteral is called when production integerLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterIntegerLiteral(ctx *IntegerLiteralContext) {}

// ExitIntegerLiteral is called when production integerLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitIntegerLiteral(ctx *IntegerLiteralContext) {}

// EnterNullLiteral is called when production nullLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterNullLiteral(ctx *NullLiteralContext) {}

// ExitNullLiteral is called when production nullLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitNullLiteral(ctx *NullLiteralContext) {}

// EnterNumericLiteral is called when production numericLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterNumericLiteral(ctx *NumericLiteralContext) {}

// ExitNumericLiteral is called when production numericLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitNumericLiteral(ctx *NumericLiteralContext) {}

// EnterBooleanLiteral is called when production booleanLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterBooleanLiteral(ctx *BooleanLiteralContext) {}

// ExitBooleanLiteral is called when production booleanLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitBooleanLiteral(ctx *BooleanLiteralContext) {}

// EnterStringLiteral is called when production stringLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterStringLiteral(ctx *StringLiteralContext) {}

// ExitStringLiteral is called when production stringLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitStringLiteral(ctx *StringLiteralContext) {}

// EnterVariableLiteral is called when production variableLiteral is entered.
func (s *BaseKsqlGrammarListener) EnterVariableLiteral(ctx *VariableLiteralContext) {}

// ExitVariableLiteral is called when production variableLiteral is exited.
func (s *BaseKsqlGrammarListener) ExitVariableLiteral(ctx *VariableLiteralContext) {}

// EnterNonReserved is called when production nonReserved is entered.
func (s *BaseKsqlGrammarListener) EnterNonReserved(ctx *NonReservedContext) {}

// ExitNonReserved is called when production nonReserved is exited.
func (s *BaseKsqlGrammarListener) ExitNonReserved(ctx *NonReservedContext) {}
