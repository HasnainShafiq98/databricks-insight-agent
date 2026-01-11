"""
Document processing module for RAG (Retrieval Augmented Generation).
Handles document loading, chunking, and embedding for FAISS index.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import re

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """Represents a chunk of a document."""
    content: str
    metadata: Dict[str, Any]
    chunk_id: str
    source: str
    
    def __repr__(self):
        return f"DocumentChunk(id={self.chunk_id}, source={self.source}, len={len(self.content)})"


class DocumentChunker:
    """
    Chunks documents intelligently for RAG.
    Supports multiple chunking strategies.
    """
    
    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        separator: str = "\n\n"
    ):
        """
        Initialize document chunker.
        
        Args:
            chunk_size: Target size of each chunk in characters
            chunk_overlap: Number of characters to overlap between chunks
            separator: Primary separator for splitting text
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separator = separator
    
    def chunk_text(self, text: str, source: str, metadata: Optional[Dict] = None) -> List[DocumentChunk]:
        """
        Chunk a text document into smaller pieces.
        
        Args:
            text: Full text to chunk
            source: Source identifier (filename, URL, etc.)
            metadata: Additional metadata to attach to chunks
            
        Returns:
            List of DocumentChunk objects
        """
        if not text or not text.strip():
            logger.warning(f"Empty text provided for source: {source}")
            return []
        
        # Initialize metadata
        base_metadata = metadata or {}
        base_metadata['source'] = source
        
        # Split by primary separator first
        paragraphs = text.split(self.separator)
        
        chunks = []
        current_chunk = ""
        chunk_index = 0
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            
            # If adding this paragraph exceeds chunk_size
            if len(current_chunk) + len(paragraph) + len(self.separator) > self.chunk_size:
                if current_chunk:
                    # Save current chunk
                    chunk_id = f"{source}_chunk_{chunk_index}"
                    chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
                    
                    chunks.append(DocumentChunk(
                        content=current_chunk.strip(),
                        metadata=chunk_metadata,
                        chunk_id=chunk_id,
                        source=source
                    ))
                    
                    chunk_index += 1
                    
                    # Start new chunk with overlap
                    if self.chunk_overlap > 0:
                        overlap_text = current_chunk[-self.chunk_overlap:]
                        current_chunk = overlap_text + self.separator + paragraph
                    else:
                        current_chunk = paragraph
                else:
                    # Paragraph itself is larger than chunk_size, split it
                    sub_chunks = self._split_large_paragraph(paragraph, source, chunk_index, base_metadata)
                    chunks.extend(sub_chunks)
                    chunk_index += len(sub_chunks)
                    current_chunk = ""
            else:
                # Add paragraph to current chunk
                if current_chunk:
                    current_chunk += self.separator + paragraph
                else:
                    current_chunk = paragraph
        
        # Add final chunk
        if current_chunk:
            chunk_id = f"{source}_chunk_{chunk_index}"
            chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
            
            chunks.append(DocumentChunk(
                content=current_chunk.strip(),
                metadata=chunk_metadata,
                chunk_id=chunk_id,
                source=source
            ))
        
        logger.info(f"Created {len(chunks)} chunks from {source}")
        return chunks
    
    def _split_large_paragraph(
        self,
        paragraph: str,
        source: str,
        start_index: int,
        base_metadata: Dict
    ) -> List[DocumentChunk]:
        """Split a large paragraph into chunks by sentences."""
        
        # Split by sentences
        sentences = re.split(r'(?<=[.!?])\s+', paragraph)
        
        chunks = []
        current_chunk = ""
        chunk_index = start_index
        
        for sentence in sentences:
            if len(current_chunk) + len(sentence) > self.chunk_size:
                if current_chunk:
                    chunk_id = f"{source}_chunk_{chunk_index}"
                    chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
                    
                    chunks.append(DocumentChunk(
                        content=current_chunk.strip(),
                        metadata=chunk_metadata,
                        chunk_id=chunk_id,
                        source=source
                    ))
                    
                    chunk_index += 1
                    current_chunk = sentence
                else:
                    # Single sentence is too large, split by words
                    word_chunks = self._split_by_words(sentence, source, chunk_index, base_metadata)
                    chunks.extend(word_chunks)
                    chunk_index += len(word_chunks)
            else:
                current_chunk += " " + sentence if current_chunk else sentence
        
        if current_chunk:
            chunk_id = f"{source}_chunk_{chunk_index}"
            chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
            
            chunks.append(DocumentChunk(
                content=current_chunk.strip(),
                metadata=chunk_metadata,
                chunk_id=chunk_id,
                source=source
            ))
        
        return chunks
    
    def _split_by_words(
        self,
        text: str,
        source: str,
        start_index: int,
        base_metadata: Dict
    ) -> List[DocumentChunk]:
        """Split text by words as last resort."""
        
        words = text.split()
        chunks = []
        current_chunk = ""
        chunk_index = start_index
        
        for word in words:
            if len(current_chunk) + len(word) + 1 > self.chunk_size:
                if current_chunk:
                    chunk_id = f"{source}_chunk_{chunk_index}"
                    chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
                    
                    chunks.append(DocumentChunk(
                        content=current_chunk.strip(),
                        metadata=chunk_metadata,
                        chunk_id=chunk_id,
                        source=source
                    ))
                    
                    chunk_index += 1
                    current_chunk = word
                else:
                    # Single word is too large, truncate
                    chunk_id = f"{source}_chunk_{chunk_index}"
                    chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
                    
                    chunks.append(DocumentChunk(
                        content=word[:self.chunk_size],
                        metadata=chunk_metadata,
                        chunk_id=chunk_id,
                        source=source
                    ))
                    chunk_index += 1
            else:
                current_chunk += " " + word if current_chunk else word
        
        if current_chunk:
            chunk_id = f"{source}_chunk_{chunk_index}"
            chunk_metadata = {**base_metadata, 'chunk_index': chunk_index}
            
            chunks.append(DocumentChunk(
                content=current_chunk.strip(),
                metadata=chunk_metadata,
                chunk_id=chunk_id,
                source=source
            ))
        
        return chunks


class DocumentLoader:
    """Loads documents from various sources."""
    
    def __init__(self, chunker: DocumentChunker):
        self.chunker = chunker
    
    def load_from_directory(
        self,
        directory: str,
        extensions: List[str] = ['.txt', '.md']
    ) -> List[DocumentChunk]:
        """
        Load and chunk all documents from a directory.
        
        Args:
            directory: Path to directory containing documents
            extensions: File extensions to process
            
        Returns:
            List of DocumentChunk objects
        """
        all_chunks = []
        
        if not os.path.exists(directory):
            logger.warning(f"Directory not found: {directory}")
            return all_chunks
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                if any(file.endswith(ext) for ext in extensions):
                    file_path = os.path.join(root, file)
                    chunks = self.load_from_file(file_path)
                    all_chunks.extend(chunks)
        
        logger.info(f"Loaded {len(all_chunks)} chunks from {directory}")
        return all_chunks
    
    def load_from_file(self, file_path: str) -> List[DocumentChunk]:
        """
        Load and chunk a single file.
        
        Args:
            file_path: Path to file
            
        Returns:
            List of DocumentChunk objects
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            
            filename = os.path.basename(file_path)
            metadata = {
                'file_path': file_path,
                'filename': filename
            }
            
            chunks = self.chunker.chunk_text(text, source=filename, metadata=metadata)
            return chunks
            
        except Exception as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            return []
    
    def load_from_dbfs(
        self,
        dbfs_path: str,
        spark=None
    ) -> List[DocumentChunk]:
        """
        Load documents from DBFS.
        
        Args:
            dbfs_path: DBFS path to document or directory
            spark: Spark session (if running in Databricks)
            
        Returns:
            List of DocumentChunk objects
        """
        # This would use dbutils in a Databricks environment
        # For now, return empty list
        logger.warning("DBFS loading not implemented in local environment")
        return []


def create_knowledge_base_documents() -> List[DocumentChunk]:
    """
    Create sample knowledge base documents for the agent.
    These documents provide context about business metrics, KPIs, and definitions.
    
    Returns:
        List of DocumentChunk objects
    """
    chunker = DocumentChunker(chunk_size=300, chunk_overlap=50)
    
    documents = [
        {
            "source": "kpi_definitions",
            "content": """
            Key Performance Indicators (KPIs) Definitions:
            
            Revenue Metrics:
            - Total Sales: Sum of all transaction amounts in a given period
            - Average Transaction Value (ATV): Total sales divided by number of transactions
            - Revenue per Customer: Total revenue divided by unique customer count
            
            Customer Metrics:
            - Customer Acquisition Cost (CAC): Marketing spend divided by new customers
            - Customer Lifetime Value (CLV): Average revenue per customer over their lifetime
            - Churn Rate: Percentage of customers who stop purchasing
            
            Product Metrics:
            - Units Sold: Total quantity of products sold
            - Inventory Turnover: How quickly inventory is sold and replaced
            - Product Margin: Profit margin per product category
            """,
            "metadata": {"category": "kpis", "type": "definitions"}
        },
        {
            "source": "data_schema",
            "content": """
            Data Schema Information:
            
            Sales Table:
            Contains transaction-level data with fields: transaction_id, customer_id, 
            product_id, amount, date, region. Each row represents a single purchase.
            Date format: YYYY-MM-DD. Amount in USD.
            
            Customers Table:
            Contains customer information: customer_id, name, email, registration_date, 
            country. One row per customer. Tracks customer demographics and registration.
            
            Products Table:
            Contains product catalog: product_id, name, category, price, stock_quantity.
            Updated in real-time as inventory changes.
            """,
            "metadata": {"category": "schema", "type": "documentation"}
        },
        {
            "source": "business_rules",
            "content": """
            Business Rules and Context:
            
            Regions:
            The company operates in 5 regions: NORTH, SOUTH, EAST, WEST, CENTRAL.
            Each region has different sales targets and pricing strategies.
            
            Product Categories:
            Main categories include Electronics, Clothing, Home & Garden, Sports, Books.
            Seasonal variations affect sales patterns, especially in Clothing and Sports.
            
            Reporting Periods:
            - Daily reports for operational metrics
            - Weekly reports for sales team
            - Monthly reports for executive dashboard
            - Quarterly reports for board meetings
            """,
            "metadata": {"category": "business", "type": "rules"}
        },
        {
            "source": "analysis_guidelines",
            "content": """
            Analysis Guidelines:
            
            Time-Based Analysis:
            When analyzing trends, always consider:
            - Seasonality: Q4 typically has 40% higher sales
            - Day of week effects: Weekends show different patterns
            - Holiday impacts: Major holidays affect purchasing behavior
            
            Segmentation:
            Customer segments: New, Active, At-Risk, Dormant
            Product segments: High-margin, Fast-moving, Slow-moving
            
            Anomaly Detection:
            Flag transactions with:
            - Amounts > $10,000 (large purchases)
            - Negative amounts (returns)
            - Duplicate transaction IDs
            """,
            "metadata": {"category": "analysis", "type": "guidelines"}
        }
    ]
    
    all_chunks = []
    for doc in documents:
        chunks = chunker.chunk_text(
            text=doc["content"],
            source=doc["source"],
            metadata=doc["metadata"]
        )
        all_chunks.extend(chunks)
    
    logger.info(f"Created {len(all_chunks)} knowledge base chunks")
    return all_chunks
