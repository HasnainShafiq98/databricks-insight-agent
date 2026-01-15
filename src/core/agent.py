"""
Main agent module for the Databricks Insight Agent.
Orchestrates query understanding, SQL generation, context retrieval, and response generation.
"""

import logging
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Types of queries the agent can handle."""
    SQL_ONLY = "sql_only"  # Direct SQL query needed
    CONTEXT_ONLY = "context_only"  # Only needs context retrieval
    BOTH = "both"  # Needs both SQL and context
    CLARIFICATION = "clarification"  # Need more information


@dataclass
class QueryAnalysis:
    """Analysis of user query."""
    query_type: QueryType
    needs_filters: bool
    missing_information: List[str]
    confidence: float
    identified_filters: Dict[str, Any]
    target_tables: List[str]


@dataclass
class AgentResponse:
    """Response from the agent."""
    success: bool
    query_type: QueryType
    sql_query: Optional[str]
    results: Optional[List[Dict[str, Any]]]
    context: Optional[str]
    insights: str
    clarification_needed: Optional[str]
    error: Optional[str]


class DatabricksInsightAgent:
    """
    AI Analytics Assistant for Databricks Lakehouse.
    
    Understands user queries, identifies filters, decides on execution strategy,
    generates safe SQL, retrieves context, and provides business-focused insights.
    """
    
    def __init__(
        self,
        databricks_client,
        schema_manager,
        sql_generator,
        context_retriever,
        security_validator,
        rate_limiter=None,
        llm_service=None
    ):
        """
        Initialize the Databricks Insight Agent.
        
        Args:
            databricks_client: Client for Databricks connection
            schema_manager: Schema manager for database structure
            sql_generator: SQL query generator
            context_retriever: FAISS-based context retrieval
            security_validator: Security validation
            rate_limiter: Rate limiter for API calls (optional)
            llm_service: LLM service for enhanced query understanding (optional)
        """
        self.databricks_client = databricks_client
        self.schema_manager = schema_manager
        self.sql_generator = sql_generator
        self.context_retriever = context_retriever
        self.security_validator = security_validator
        self.rate_limiter = rate_limiter
        self.llm_service = llm_service
    
    def process_query(self, user_query: str, user_id: str = "default") -> AgentResponse:
        """
        Process a user query end-to-end.
        
        Args:
            user_query: Natural language query from user
            user_id: User identifier for rate limiting
            
        Returns:
            AgentResponse with results and insights
        """
        logger.info(f"Processing query: {user_query}")
        
        # Step 1: Security validation
        is_valid, error_msg = self.security_validator.validate_query(user_query)
        if not is_valid:
            logger.warning(f"Security validation failed: {error_msg}")
            return AgentResponse(
                success=False,
                query_type=QueryType.CLARIFICATION,
                sql_query=None,
                results=None,
                context=None,
                insights="",
                clarification_needed=None,
                error=f"Security validation failed: {error_msg}"
            )
        
        # Step 2: Rate limiting
        if self.rate_limiter:
            rate_ok, rate_msg = self.rate_limiter.check_rate_limit(user_id)
            if not rate_ok:
                logger.warning(f"Rate limit exceeded for user {user_id}")
                return AgentResponse(
                    success=False,
                    query_type=QueryType.CLARIFICATION,
                    sql_query=None,
                    results=None,
                    context=None,
                    insights="",
                    clarification_needed=None,
                    error=rate_msg
                )
        
        # Step 3: Analyze query to understand intent
        query_analysis = self.analyze_query(user_query)
        
        # Step 4: Check if clarification is needed
        if query_analysis.query_type == QueryType.CLARIFICATION:
            clarification = self._generate_clarification(query_analysis)
            return AgentResponse(
                success=False,
                query_type=QueryType.CLARIFICATION,
                sql_query=None,
                results=None,
                context=None,
                insights="",
                clarification_needed=clarification,
                error=None
            )
        
        # Step 5: Retrieve context if needed
        context = None
        if query_analysis.query_type in [QueryType.CONTEXT_ONLY, QueryType.BOTH]:
            context = self.context_retriever.get_context(user_query, top_k=3)
        
        # Step 6: Generate and execute SQL if needed
        sql_query = None
        results = None
        if query_analysis.query_type in [QueryType.SQL_ONLY, QueryType.BOTH]:
            sql_query = self._generate_safe_sql(user_query, query_analysis, context)
            
            if sql_query:
                # Validate SQL before execution
                sql_valid, sql_error = self.security_validator.validate_sql(sql_query)
                if not sql_valid:
                    logger.error(f"Generated SQL failed validation: {sql_error}")
                    return AgentResponse(
                        success=False,
                        query_type=query_analysis.query_type,
                        sql_query=sql_query,
                        results=None,
                        context=context,
                        insights="",
                        clarification_needed=None,
                        error=f"SQL validation failed: {sql_error}"
                    )
                
                # Execute SQL
                try:
                    results = self.databricks_client.execute_query(sql_query)
                except Exception as e:
                    logger.error(f"SQL execution failed: {e}")
                    return AgentResponse(
                        success=False,
                        query_type=query_analysis.query_type,
                        sql_query=sql_query,
                        results=None,
                        context=context,
                        insights="",
                        clarification_needed=None,
                        error=f"Query execution failed: {str(e)}"
                    )
        
        # Step 7: Generate insights from results and context
        insights = self._generate_insights(user_query, results, context, query_analysis, sql_query=sql_query)
        
        return AgentResponse(
            success=True,
            query_type=query_analysis.query_type,
            sql_query=sql_query,
            results=results,
            context=context,
            insights=insights,
            clarification_needed=None,
            error=None
        )
    
    def analyze_query(self, user_query: str) -> QueryAnalysis:
        """
        Analyze user query to understand intent and requirements.
        
        Args:
            user_query: Natural language query
            
        Returns:
            QueryAnalysis with query understanding
        """
        query_lower = user_query.lower()
        
        # Identify target tables
        target_tables = []
        for table_name in self.schema_manager.get_all_tables():
            if table_name.lower() in query_lower:
                target_tables.append(table_name)
        
        # Check if we have tables to work with
        if not target_tables and any(
            keyword in query_lower 
            for keyword in ['show', 'get', 'find', 'list', 'count', 'sum', 'average']
        ):
            # Might need clarification on which table
            return QueryAnalysis(
                query_type=QueryType.CLARIFICATION,
                needs_filters=False,
                missing_information=["table name"],
                confidence=0.5,
                identified_filters={},
                target_tables=[]
            )
        
        # Determine query type
        query_type = QueryType.BOTH  # Default to both
        
        # Check for data retrieval keywords (needs SQL)
        sql_keywords = ['show', 'get', 'find', 'list', 'count', 'sum', 'average', 'total', 'calculate']
        needs_sql = any(keyword in query_lower for keyword in sql_keywords)
        
        # Check for explanation/documentation keywords (needs context)
        context_keywords = ['explain', 'what is', 'how to', 'describe', 'tell me about']
        needs_context = any(keyword in query_lower for keyword in context_keywords)
        
        if needs_sql and not needs_context:
            query_type = QueryType.SQL_ONLY
        elif needs_context and not needs_sql:
            query_type = QueryType.CONTEXT_ONLY
        elif needs_sql and needs_context:
            query_type = QueryType.BOTH
        
        # Identify potential filters
        identified_filters = {}
        # This is simplified - in production, use NER or LLM
        
        return QueryAnalysis(
            query_type=query_type,
            needs_filters=True,
            missing_information=[],
            confidence=0.8,
            identified_filters=identified_filters,
            target_tables=target_tables
        )
    
    def _generate_safe_sql(
        self, 
        user_query: str, 
        analysis: QueryAnalysis,
        context: Optional[str]
    ) -> Optional[str]:
        """Generate safe SQL query from analysis."""
        if not analysis.target_tables:
            logger.warning("No target tables identified")
            return None
        
        # Use the first identified table
        table_name = analysis.target_tables[0]
        
        # Try LLM-powered SQL generation if available
        if self.llm_service:
            try:
                schema_info = self.schema_manager.get_schema_summary()
                sql = self.llm_service.generate_sql_from_query(
                    user_query=user_query,
                    schema_info=schema_info,
                    context=context
                )
                
                if sql:
                    logger.info("Generated SQL using Mistral AI")
                    return sql
            except Exception as e:
                logger.warning(f"LLM SQL generation failed, falling back to rule-based: {e}")
        
        # Fallback to rule-based SQL generation
        # Parse query intent
        intent = self.sql_generator.parse_query_intent(user_query, context)
        intent['table_name'] = table_name
        
        # Add identified filters
        if analysis.identified_filters:
            intent['filters'] = analysis.identified_filters
        
        # Generate SQL
        sql = self.sql_generator.generate_sql(**intent)
        return sql
    
    def _generate_insights(
        self,
        user_query: str,
        results: Optional[List[Dict[str, Any]]],
        context: Optional[str],
        analysis: QueryAnalysis,
        sql_query: Optional[str] = None
    ) -> str:
        """
        Generate business-focused insights from results and context.
        
        Args:
            user_query: Original user query
            results: SQL query results
            context: Retrieved context
            analysis: Query analysis
            sql_query: SQL query that was executed
            
        Returns:
            Human-readable insights
        """
        # Try LLM-powered insights generation if available
        if self.llm_service:
            try:
                insights = self.llm_service.generate_insights(
                    user_query=user_query,
                    sql_query=sql_query,
                    results=results,
                    context=context
                )
                logger.info("Generated insights using Mistral AI")
                return insights
            except Exception as e:
                logger.warning(f"LLM insights generation failed, falling back to rule-based: {e}")
        
        # Fallback to rule-based insights generation
        insights = []
        
        # Add context-based insights
        if context:
            insights.append("**Context:**")
            insights.append(context[:500])  # Limit context length
        
        # Add data-based insights
        if results:
            insights.append("\n**Analysis:**")
            insights.append(f"Found {len(results)} record(s).")
            
            if len(results) > 0:
                # Show sample data
                insights.append("\n**Sample Data:**")
                for i, row in enumerate(results[:3]):  # Show first 3 rows
                    insights.append(f"Record {i+1}: {row}")
                
                # Basic statistical insights
                if len(results) > 1:
                    # Try to identify numeric columns for stats
                    numeric_cols = []
                    for key, value in results[0].items():
                        if isinstance(value, (int, float)):
                            numeric_cols.append(key)
                    
                    if numeric_cols:
                        insights.append("\n**Key Metrics:**")
                        for col in numeric_cols[:3]:  # Limit to 3 columns
                            values = [row[col] for row in results if col in row and row[col] is not None]
                            if values:
                                avg_val = sum(values) / len(values)
                                max_val = max(values)
                                min_val = min(values)
                                insights.append(f"- {col}: Average = {avg_val:.2f}, Min = {min_val}, Max = {max_val}")
        
        if not insights:
            insights.append("No specific insights available. Please refine your query.")
        
        return "\n".join(insights)
    
    def _generate_clarification(self, analysis: QueryAnalysis) -> str:
        """Generate clarification message for user."""
        if "table name" in analysis.missing_information:
            available_tables = self.schema_manager.get_all_tables()
            return (
                f"I need more information to process your query. "
                f"Which table would you like to query? Available tables: {', '.join(available_tables)}"
            )
        
        missing = ", ".join(analysis.missing_information)
        return f"I need more information: {missing}. Please provide additional details."
