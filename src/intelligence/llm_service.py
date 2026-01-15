"""
LLM service for enhanced query understanding and response generation.
Supports Mistral AI for natural language processing.
"""

import logging
import os
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    usage: Optional[Dict[str, int]] = None


class MistralLLMService:
    """
    LLM service using Mistral AI for query understanding and response generation.
    """
    
    def __init__(
        self,
        api_key: str,
        model: str = "mistral-large-latest",
        temperature: float = 0.1,
        max_tokens: int = 2000
    ):
        """
        Initialize Mistral AI service.
        
        Args:
            api_key: Mistral API key
            model: Model to use (mistral-large-latest, mistral-medium, mistral-small, etc.)
            temperature: Sampling temperature (0-1)
            max_tokens: Maximum tokens in response
        """
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.client = None
        
        # Initialize Mistral client (using new API)
        try:
            from mistralai import Mistral
            self.client = Mistral(api_key=api_key)
            logger.info(f"Initialized Mistral AI with model: {model}")
        except ImportError:
            logger.error("mistralai package not installed. Install with: pip install mistralai")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Mistral client: {e}")
            raise
    
    def generate_sql_from_query(
        self,
        user_query: str,
        schema_info: str,
        context: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate SQL query from natural language using Mistral AI.
        
        Args:
            user_query: User's natural language query
            schema_info: Database schema information
            context: Additional context from knowledge base
            
        Returns:
            Generated SQL query or None if failed
        """
        prompt = self._build_sql_generation_prompt(user_query, schema_info, context)
        
        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are an expert SQL query generator for Databricks. Generate only valid SQL queries without explanations."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            response = self.client.chat.complete(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            sql = response.choices[0].message.content.strip()
            
            # Extract SQL from markdown code blocks if present
            if "```sql" in sql:
                sql = sql.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql:
                sql = sql.split("```")[1].split("```")[0].strip()
            
            logger.info(f"Generated SQL: {sql}")
            return sql
            
        except Exception as e:
            logger.error(f"Failed to generate SQL with Mistral AI: {e}")
            return None
    
    def generate_insights(
        self,
        user_query: str,
        sql_query: Optional[str],
        results: Optional[List[Dict[str, Any]]],
        context: Optional[str]
    ) -> str:
        """
        Generate business insights from query results using Mistral AI.
        
        Args:
            user_query: Original user query
            sql_query: SQL query that was executed
            results: Query results
            context: Retrieved context from knowledge base
            
        Returns:
            Generated insights text
        """
        prompt = self._build_insights_prompt(user_query, sql_query, results, context)
        
        try:
            messages = [
                {
                    "role": "system",
                    "content": "You are a business intelligence analyst. Provide clear, actionable insights from data."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            response = self.client.chat.complete(
                model=self.model,
                messages=messages,
                temperature=0.3,  # Slightly higher for creative insights
                max_tokens=self.max_tokens
            )
            
            insights = response.choices[0].message.content.strip()
            logger.info("Generated insights with Mistral AI")
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights with Mistral AI: {e}")
            return self._generate_fallback_insights(user_query, results)
    
    def parse_query_intent(
        self,
        user_query: str,
        schema_info: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parse user query to understand intent using Mistral AI.
        
        Args:
            user_query: User's natural language query
            schema_info: Available schema information
            context: Additional context
            
        Returns:
            Dictionary with parsed intent
        """
        prompt = f"""
Analyze this user query and extract the intent:

Query: {user_query}

Available Schema:
{schema_info}

Context:
{context if context else 'None'}

Return a JSON object with:
- table_name: which table to query
- columns: list of columns needed (or null for all)
- filters: dictionary of column: value filters
- aggregations: dictionary of column: aggregation_function (COUNT, SUM, AVG, etc.)
- group_by: list of columns to group by
- order_by: list of [column, direction] pairs
- limit: number of rows to return

Return only valid JSON, no explanations.
"""
        
        try:
            import json
            
            messages = [
                {
                    "role": "system",
                    "content": "You are a query parser. Return only valid JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            response = self.client.chat.complete(
                model=self.model,
                messages=messages,
                temperature=0.0,  # Deterministic parsing
                max_tokens=1000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Extract JSON from markdown if present
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            intent = json.loads(content)
            logger.info(f"Parsed query intent: {intent}")
            return intent
            
        except Exception as e:
            logger.error(f"Failed to parse query intent with Mistral AI: {e}")
            return {
                'table_name': None,
                'columns': None,
                'filters': {},
                'aggregations': None,
                'group_by': None,
                'order_by': None,
                'limit': None
            }
    
    def _build_sql_generation_prompt(
        self,
        user_query: str,
        schema_info: str,
        context: Optional[str]
    ) -> str:
        """Build prompt for SQL generation."""
        prompt = f"""
Generate a SQL query for Databricks SQL to answer this question:

Question: {user_query}

Available Schema:
{schema_info}
"""
        
        if context:
            prompt += f"""
Additional Context:
{context}
"""
        
        prompt += """
Rules:
1. Use only columns that exist in the schema
2. Wrap column names with spaces in backticks: `Column Name`
3. Use proper aggregations (SUM, AVG, COUNT, etc.) when needed
4. Include appropriate GROUP BY for aggregations
5. Add ORDER BY for meaningful sorting
6. Return ONLY the SQL query, no explanations

Generate the SQL query:
"""
        return prompt
    
    def _build_insights_prompt(
        self,
        user_query: str,
        sql_query: Optional[str],
        results: Optional[List[Dict[str, Any]]],
        context: Optional[str]
    ) -> str:
        """Build prompt for insights generation."""
        prompt = f"""
Generate business insights for this analysis:

User Question: {user_query}

"""
        
        if sql_query:
            prompt += f"SQL Query: {sql_query}\n\n"
        
        if results:
            # Limit results to first 10 rows for prompt
            results_preview = results[:10]
            prompt += f"Results ({len(results)} total rows):\n"
            for i, row in enumerate(results_preview, 1):
                prompt += f"{i}. {row}\n"
            if len(results) > 10:
                prompt += f"... and {len(results) - 10} more rows\n"
            prompt += "\n"
        
        if context:
            prompt += f"Business Context:\n{context}\n\n"
        
        prompt += """
Provide:
1. Key findings from the data
2. Business implications
3. Actionable recommendations
4. Any notable trends or patterns

Keep the insights concise and business-focused.
"""
        return prompt
    
    def _generate_fallback_insights(
        self,
        user_query: str,
        results: Optional[List[Dict[str, Any]]]
    ) -> str:
        """Generate simple insights without LLM as fallback."""
        if not results:
            return "No results found for your query."
        
        insights = f"Query returned {len(results)} result(s).\n\n"
        
        if len(results) > 0:
            insights += "Sample data:\n"
            for i, row in enumerate(results[:5], 1):
                insights += f"{i}. {row}\n"
        
        return insights


def create_llm_service(
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    provider: str = "mistral"
) -> Optional[MistralLLMService]:
    """
    Factory function to create LLM service.
    
    Args:
        api_key: API key (will use env var if not provided)
        model: Model name (will use env var or default if not provided)
        provider: LLM provider (currently only 'mistral' supported)
        
    Returns:
        LLM service instance or None if not configured
    """
    if provider != "mistral":
        logger.warning(f"Unsupported LLM provider: {provider}, only 'mistral' is supported")
        return None
    
    api_key = api_key or os.getenv('MISTRAL_API_KEY')
    if not api_key:
        logger.info("No Mistral API key provided. LLM features will be disabled.")
        return None
    
    model = model or os.getenv('MISTRAL_MODEL', 'mistral-large-latest')
    
    try:
        service = MistralLLMService(api_key=api_key, model=model)
        return service
    except Exception as e:
        logger.error(f"Failed to create LLM service: {e}")
        return None
