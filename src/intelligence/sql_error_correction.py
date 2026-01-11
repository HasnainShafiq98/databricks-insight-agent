"""
SQL error correction and retry logic module.
Intelligently fixes SQL errors and retries queries with corrections.
"""

import logging
import re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SQLError:
    """Represents a SQL execution error."""
    error_message: str
    error_type: str
    sql_query: str
    
    
@dataclass
class SQLCorrection:
    """Represents a SQL correction attempt."""
    original_sql: str
    corrected_sql: str
    correction_type: str
    confidence: float


class SQLErrorAnalyzer:
    """Analyzes SQL errors and determines correction strategies."""
    
    # Common error patterns and their corrections
    ERROR_PATTERNS = {
        'column_not_found': {
            'patterns': [
                r"column '?(\w+)'? (does not exist|not found|cannot be resolved)",
                r"Unknown column '?(\w+)'?",
                r"no such column: (\w+)"
            ],
            'correction': 'suggest_column_name'
        },
        'table_not_found': {
            'patterns': [
                r"table '?(\w+)'? (does not exist|not found|cannot be resolved)",
                r"Unknown table '?(\w+)'?",
                r"no such table: (\w+)"
            ],
            'correction': 'suggest_table_name'
        },
        'syntax_error': {
            'patterns': [
                r"syntax error near '?(.+?)'?",
                r"ParseException",
                r"mismatched input"
            ],
            'correction': 'fix_syntax'
        },
        'type_mismatch': {
            'patterns': [
                r"type mismatch",
                r"cannot cast",
                r"incompatible types"
            ],
            'correction': 'fix_type_cast'
        },
        'aggregate_error': {
            'patterns': [
                r"not a GROUP BY expression",
                r"must appear in the GROUP BY clause"
            ],
            'correction': 'fix_group_by'
        }
    }
    
    def analyze_error(self, error_message: str, sql_query: str) -> Optional[SQLError]:
        """
        Analyze a SQL error message.
        
        Args:
            error_message: Error message from database
            sql_query: The SQL query that caused the error
            
        Returns:
            SQLError object or None if can't analyze
        """
        for error_type, error_info in self.ERROR_PATTERNS.items():
            for pattern in error_info['patterns']:
                if re.search(pattern, error_message, re.IGNORECASE):
                    return SQLError(
                        error_message=error_message,
                        error_type=error_type,
                        sql_query=sql_query
                    )
        
        # Unknown error type
        return SQLError(
            error_message=error_message,
            error_type='unknown',
            sql_query=sql_query
        )


class SQLCorrector:
    """
    Corrects SQL queries based on error analysis and schema knowledge.
    """
    
    def __init__(self, schema_manager):
        """
        Initialize SQL corrector.
        
        Args:
            schema_manager: SchemaManager instance for schema validation
        """
        self.schema_manager = schema_manager
        self.analyzer = SQLErrorAnalyzer()
    
    def correct_query(
        self,
        sql_query: str,
        error_message: str,
        max_attempts: int = 3
    ) -> Optional[SQLCorrection]:
        """
        Attempt to correct a SQL query based on error.
        
        Args:
            sql_query: Original SQL query
            error_message: Error message from execution
            max_attempts: Maximum correction attempts
            
        Returns:
            SQLCorrection object or None if can't correct
        """
        sql_error = self.analyzer.analyze_error(error_message, sql_query)
        if not sql_error:
            return None
        
        logger.info(f"Attempting to correct SQL error: {sql_error.error_type}")
        
        # Route to appropriate correction method
        correction_method = {
            'column_not_found': self._correct_column_name,
            'table_not_found': self._correct_table_name,
            'syntax_error': self._correct_syntax,
            'type_mismatch': self._correct_type_cast,
            'aggregate_error': self._correct_group_by
        }.get(sql_error.error_type)
        
        if correction_method:
            return correction_method(sql_error)
        
        return None
    
    def _correct_column_name(self, sql_error: SQLError) -> Optional[SQLCorrection]:
        """Correct column name errors by suggesting similar column names."""
        
        # Extract the problematic column name
        pattern = r"column '?(\w+)'?"
        match = re.search(pattern, sql_error.error_message, re.IGNORECASE)
        if not match:
            return None
        
        wrong_column = match.group(1)
        
        # Find the table being queried
        table_pattern = r"FROM\s+(\w+)"
        table_match = re.search(table_pattern, sql_error.sql_query, re.IGNORECASE)
        if not table_match:
            return None
        
        table_name = table_match.group(1)
        table_columns = self.schema_manager.get_table_columns(table_name)
        
        if not table_columns:
            return None
        
        # Find the most similar column name
        similar_column = self._find_similar_string(wrong_column, table_columns)
        
        if similar_column:
            corrected_sql = sql_error.sql_query.replace(wrong_column, similar_column)
            
            return SQLCorrection(
                original_sql=sql_error.sql_query,
                corrected_sql=corrected_sql,
                correction_type='column_name',
                confidence=0.8
            )
        
        return None
    
    def _correct_table_name(self, sql_error: SQLError) -> Optional[SQLCorrection]:
        """Correct table name errors by suggesting similar table names."""
        
        # Extract the problematic table name
        pattern = r"table '?(\w+)'?"
        match = re.search(pattern, sql_error.error_message, re.IGNORECASE)
        if not match:
            return None
        
        wrong_table = match.group(1)
        available_tables = self.schema_manager.get_all_tables()
        
        # Find the most similar table name
        similar_table = self._find_similar_string(wrong_table, available_tables)
        
        if similar_table:
            corrected_sql = re.sub(
                rf"\b{wrong_table}\b",
                similar_table,
                sql_error.sql_query,
                flags=re.IGNORECASE
            )
            
            return SQLCorrection(
                original_sql=sql_error.sql_query,
                corrected_sql=corrected_sql,
                correction_type='table_name',
                confidence=0.8
            )
        
        return None
    
    def _correct_syntax(self, sql_error: SQLError) -> Optional[SQLCorrection]:
        """Attempt to fix common syntax errors."""
        
        corrected_sql = sql_error.sql_query
        
        # Common syntax fixes
        fixes = [
            # Missing comma between columns
            (r"(\w+)\s+(\w+)\s+FROM", r"\1, \2 FROM"),
            # Double quotes instead of single quotes for strings
            (r'"([^"]+)"', r"'\1'"),
            # Missing GROUP BY for aggregates
            (r"(SELECT.*?\bCOUNT\b.*?FROM.*?)(?!.*GROUP BY)", r"\1 GROUP BY 1"),
        ]
        
        for pattern, replacement in fixes:
            if re.search(pattern, corrected_sql, re.IGNORECASE):
                corrected_sql = re.sub(pattern, replacement, corrected_sql, flags=re.IGNORECASE)
                
                if corrected_sql != sql_error.sql_query:
                    return SQLCorrection(
                        original_sql=sql_error.sql_query,
                        corrected_sql=corrected_sql,
                        correction_type='syntax',
                        confidence=0.6
                    )
        
        return None
    
    def _correct_type_cast(self, sql_error: SQLError) -> Optional[SQLCorrection]:
        """Add explicit type casts where needed."""
        
        # This is a simple implementation
        # In practice, you'd need more sophisticated type inference
        
        corrected_sql = sql_error.sql_query
        
        # Common type cast fixes
        # Add CAST when comparing different types
        if 'type mismatch' in sql_error.error_message.lower():
            # This is a placeholder - real implementation would be more complex
            pass
        
        return None
    
    def _correct_group_by(self, sql_error: SQLError) -> Optional[SQLCorrection]:
        """Fix GROUP BY clause issues."""
        
        # Extract SELECT columns that aren't aggregated
        select_pattern = r"SELECT\s+(.*?)\s+FROM"
        select_match = re.search(select_pattern, sql_error.sql_query, re.IGNORECASE | re.DOTALL)
        
        if not select_match:
            return None
        
        select_clause = select_match.group(1)
        
        # Find non-aggregated columns
        # This is simplified - real implementation would parse SQL properly
        columns = [col.strip() for col in select_clause.split(',')]
        non_aggregate_cols = [
            col for col in columns 
            if not any(agg in col.upper() for agg in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])
        ]
        
        if non_aggregate_cols:
            # Add or update GROUP BY clause
            group_by_clause = f"GROUP BY {', '.join(non_aggregate_cols)}"
            
            if 'GROUP BY' in sql_error.sql_query.upper():
                # Replace existing GROUP BY
                corrected_sql = re.sub(
                    r'GROUP BY.*?(?=ORDER BY|LIMIT|$)',
                    group_by_clause + ' ',
                    sql_error.sql_query,
                    flags=re.IGNORECASE
                )
            else:
                # Add GROUP BY before ORDER BY or LIMIT
                if 'ORDER BY' in sql_error.sql_query.upper():
                    corrected_sql = re.sub(
                        r'ORDER BY',
                        f'{group_by_clause} ORDER BY',
                        sql_error.sql_query,
                        flags=re.IGNORECASE
                    )
                elif 'LIMIT' in sql_error.sql_query.upper():
                    corrected_sql = re.sub(
                        r'LIMIT',
                        f'{group_by_clause} LIMIT',
                        sql_error.sql_query,
                        flags=re.IGNORECASE
                    )
                else:
                    corrected_sql = sql_error.sql_query.rstrip() + ' ' + group_by_clause
            
            if corrected_sql != sql_error.sql_query:
                return SQLCorrection(
                    original_sql=sql_error.sql_query,
                    corrected_sql=corrected_sql,
                    correction_type='group_by',
                    confidence=0.7
                )
        
        return None
    
    def _find_similar_string(self, target: str, candidates: List[str]) -> Optional[str]:
        """
        Find the most similar string from candidates.
        Uses Levenshtein distance.
        """
        if not candidates:
            return None
        
        def levenshtein_distance(s1: str, s2: str) -> int:
            """Calculate Levenshtein distance between two strings."""
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)
            
            if len(s2) == 0:
                return len(s1)
            
            previous_row = range(len(s2) + 1)
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            
            return previous_row[-1]
        
        # Calculate distances
        distances = [
            (candidate, levenshtein_distance(target.lower(), candidate.lower()))
            for candidate in candidates
        ]
        
        # Sort by distance
        distances.sort(key=lambda x: x[1])
        
        # Return closest match if distance is reasonable
        best_match, distance = distances[0]
        max_distance = len(target) // 2  # Allow up to 50% difference
        
        if distance <= max_distance:
            return best_match
        
        return None


class RetryableQueryExecutor:
    """
    Executes SQL queries with automatic error correction and retry logic.
    """
    
    def __init__(self, databricks_client, sql_corrector: SQLCorrector):
        """
        Initialize retryable query executor.
        
        Args:
            databricks_client: Databricks client for query execution
            sql_corrector: SQLCorrector instance
        """
        self.databricks_client = databricks_client
        self.sql_corrector = sql_corrector
    
    def execute_with_retry(
        self,
        sql_query: str,
        max_retries: int = 3
    ) -> Tuple[bool, Optional[List[Dict]], List[str]]:
        """
        Execute SQL query with automatic retry and correction.
        
        Args:
            sql_query: SQL query to execute
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple of (success, results, correction_log)
        """
        correction_log = []
        current_query = sql_query
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Executing query (attempt {attempt + 1}/{max_retries})")
                results = self.databricks_client.execute_query(current_query)
                
                if attempt > 0:
                    correction_log.append(f"✅ Query succeeded after {attempt} correction(s)")
                
                return True, results, correction_log
                
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Query failed: {error_message}")
                
                correction_log.append(f"❌ Attempt {attempt + 1} failed: {error_message[:100]}")
                
                # Try to correct the query
                correction = self.sql_corrector.correct_query(current_query, error_message)
                
                if correction and attempt < max_retries - 1:
                    logger.info(f"Attempting correction: {correction.correction_type}")
                    correction_log.append(
                        f"🔧 Applied correction: {correction.correction_type} "
                        f"(confidence: {correction.confidence:.1%})"
                    )
                    current_query = correction.corrected_sql
                else:
                    # Can't correct or out of retries
                    correction_log.append("❌ Unable to correct query")
                    return False, None, correction_log
        
        return False, None, correction_log
