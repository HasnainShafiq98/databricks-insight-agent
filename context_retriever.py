"""
Context retrieval module using FAISS for semantic search.
Retrieves relevant documentation and context for query understanding.
"""

import logging
import os
import pickle
from typing import List, Dict, Any, Optional
import numpy as np

try:
    import faiss
    from sentence_transformers import SentenceTransformer
except ImportError:
    faiss = None
    SentenceTransformer = None

logger = logging.getLogger(__name__)


class Document:
    """Represents a document in the knowledge base."""
    
    def __init__(self, content: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize a document.
        
        Args:
            content: Document text content
            metadata: Optional metadata (e.g., source, table_name, category)
        """
        self.content = content
        self.metadata = metadata or {}
    
    def __repr__(self):
        return f"Document(content='{self.content[:50]}...', metadata={self.metadata})"


class ContextRetriever:
    """Retrieves relevant context using FAISS vector search."""
    
    def __init__(
        self,
        embedding_model: str = "all-MiniLM-L6-v2",
        index_path: Optional[str] = None
    ):
        """
        Initialize context retriever.
        
        Args:
            embedding_model: Name of the sentence transformer model
            index_path: Path to save/load FAISS index
        """
        if faiss is None or SentenceTransformer is None:
            raise ImportError(
                "FAISS and sentence-transformers are required. "
                "Install with: pip install faiss-cpu sentence-transformers"
            )
        
        self.embedding_model_name = embedding_model
        self.model = SentenceTransformer(embedding_model)
        self.index_path = index_path
        self.index = None
        self.documents: List[Document] = []
        self.dimension = self.model.get_sentence_embedding_dimension()
        
        # Load existing index if available
        if index_path and os.path.exists(index_path):
            self.load_index()
    
    def add_documents(self, documents: List[Document]):
        """
        Add documents to the knowledge base.
        
        Args:
            documents: List of Document objects to add
        """
        if not documents:
            logger.warning("No documents to add")
            return
        
        # Generate embeddings for documents
        texts = [doc.content for doc in documents]
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        
        # Create or update FAISS index
        if self.index is None:
            self.index = faiss.IndexFlatL2(self.dimension)
        
        # Add to index
        self.index.add(embeddings.astype('float32'))
        self.documents.extend(documents)
        
        logger.info(f"Added {len(documents)} documents to index. Total: {len(self.documents)}")
    
    def search(self, query: str, top_k: int = 3) -> List[tuple[Document, float]]:
        """
        Search for relevant documents.
        
        Args:
            query: Search query
            top_k: Number of top results to return
            
        Returns:
            List of (Document, score) tuples
        """
        if self.index is None or len(self.documents) == 0:
            logger.warning("No documents in index")
            return []
        
        # Generate query embedding
        query_embedding = self.model.encode([query], convert_to_numpy=True)
        
        # Search in FAISS index
        top_k = min(top_k, len(self.documents))
        distances, indices = self.index.search(query_embedding.astype('float32'), top_k)
        
        # Retrieve documents with scores
        results = []
        for i, idx in enumerate(indices[0]):
            if idx < len(self.documents):
                doc = self.documents[idx]
                score = float(distances[0][i])
                results.append((doc, score))
        
        logger.info(f"Found {len(results)} relevant documents")
        return results
    
    def get_context(self, query: str, top_k: int = 3, max_length: int = 2000) -> str:
        """
        Get context string from relevant documents.
        
        Args:
            query: Search query
            top_k: Number of documents to retrieve
            max_length: Maximum length of context string
            
        Returns:
            Concatenated context from relevant documents
        """
        results = self.search(query, top_k)
        
        if not results:
            return ""
        
        context_parts = []
        total_length = 0
        
        for doc, score in results:
            content = doc.content
            if total_length + len(content) > max_length:
                # Truncate to fit max_length
                remaining = max_length - total_length
                content = content[:remaining]
            
            context_parts.append(content)
            total_length += len(content)
            
            if total_length >= max_length:
                break
        
        context = "\n\n".join(context_parts)
        logger.info(f"Retrieved context of length {len(context)}")
        return context
    
    def save_index(self, path: Optional[str] = None):
        """
        Save FAISS index and documents to disk.
        
        Args:
            path: Path to save index (uses self.index_path if not provided)
        """
        if self.index is None:
            logger.warning("No index to save")
            return
        
        save_path = path or self.index_path
        if not save_path:
            logger.error("No save path provided")
            return
        
        # Save FAISS index
        faiss.write_index(self.index, save_path)
        
        # Save documents separately
        docs_path = save_path + ".docs"
        with open(docs_path, 'wb') as f:
            pickle.dump(self.documents, f)
        
        logger.info(f"Saved index to {save_path}")
    
    def load_index(self, path: Optional[str] = None):
        """
        Load FAISS index and documents from disk.
        
        Args:
            path: Path to load index from (uses self.index_path if not provided)
        """
        load_path = path or self.index_path
        if not load_path:
            logger.error("No load path provided")
            return
        
        if not os.path.exists(load_path):
            logger.warning(f"Index file not found: {load_path}")
            return
        
        # Load FAISS index
        self.index = faiss.read_index(load_path)
        
        # Load documents
        docs_path = load_path + ".docs"
        if os.path.exists(docs_path):
            with open(docs_path, 'rb') as f:
                self.documents = pickle.load(f)
        
        logger.info(f"Loaded index from {load_path} with {len(self.documents)} documents")
    
    def clear(self):
        """Clear all documents and reset index."""
        self.index = None
        self.documents = []
        logger.info("Cleared index and documents")


def create_sample_documents() -> List[Document]:
    """Create sample documents about Databricks schema."""
    documents = [
        Document(
            content="The sales table contains transaction data with columns: transaction_id, customer_id, product_id, amount, date, region.",
            metadata={"table": "sales", "type": "schema"}
        ),
        Document(
            content="The customers table has customer information including: customer_id, name, email, registration_date, country.",
            metadata={"table": "customers", "type": "schema"}
        ),
        Document(
            content="The products table stores product details: product_id, name, category, price, stock_quantity.",
            metadata={"table": "products", "type": "schema"}
        ),
        Document(
            content="To calculate total sales, use SUM(amount) from the sales table. Group by region for regional analysis.",
            metadata={"type": "query_pattern"}
        ),
        Document(
            content="For customer analysis, join customers and sales tables on customer_id. This shows purchase behavior.",
            metadata={"type": "query_pattern"}
        ),
    ]
    return documents
