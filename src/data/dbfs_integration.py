"""
DBFS (Databricks File System) integration module.
Handles storing and retrieving FAISS indices and documents in DBFS.
"""

import os
import logging
import pickle
from typing import Optional, List
import tempfile

logger = logging.getLogger(__name__)


class DBFSStorage:
    """
    Manages FAISS index and document storage in DBFS.
    Provides seamless integration between local development and Databricks environment.
    """
    
    def __init__(
        self,
        dbfs_mount_point: str = "/dbfs/mnt/faiss",
        local_cache_dir: str = "./data/cache"
    ):
        """
        Initialize DBFS storage.
        
        Args:
            dbfs_mount_point: DBFS mount point for storage
            local_cache_dir: Local directory for caching (when not in Databricks)
        """
        self.dbfs_mount_point = dbfs_mount_point
        self.local_cache_dir = local_cache_dir
        self.is_databricks = self._check_databricks_environment()
        
        # Create cache directory if needed
        if not self.is_databricks:
            os.makedirs(local_cache_dir, exist_ok=True)
            logger.info(f"Using local cache: {local_cache_dir}")
        else:
            logger.info(f"Using DBFS storage: {dbfs_mount_point}")
    
    def _check_databricks_environment(self) -> bool:
        """Check if running in Databricks environment."""
        return os.path.exists('/databricks/spark')
    
    def save_faiss_index(
        self,
        index,
        index_name: str = "faiss_index.index"
    ) -> bool:
        """
        Save FAISS index to DBFS or local storage.
        
        Args:
            index: FAISS index object
            index_name: Name of the index file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import faiss
            
            if self.is_databricks:
                # Save to DBFS
                index_path = os.path.join(self.dbfs_mount_point, index_name)
                os.makedirs(os.path.dirname(index_path), exist_ok=True)
                faiss.write_index(index, index_path)
                logger.info(f"✅ FAISS index saved to DBFS: {index_path}")
            else:
                # Save to local cache
                index_path = os.path.join(self.local_cache_dir, index_name)
                os.makedirs(os.path.dirname(index_path), exist_ok=True)
                faiss.write_index(index, index_path)
                logger.info(f"✅ FAISS index saved locally: {index_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save FAISS index: {e}")
            return False
    
    def load_faiss_index(self, index_name: str = "faiss_index.index"):
        """
        Load FAISS index from DBFS or local storage.
        
        Args:
            index_name: Name of the index file
            
        Returns:
            FAISS index object or None if not found
        """
        try:
            import faiss
            
            if self.is_databricks:
                index_path = os.path.join(self.dbfs_mount_point, index_name)
            else:
                index_path = os.path.join(self.local_cache_dir, index_name)
            
            if not os.path.exists(index_path):
                logger.warning(f"FAISS index not found: {index_path}")
                return None
            
            index = faiss.read_index(index_path)
            logger.info(f"✅ FAISS index loaded from: {index_path}")
            return index
            
        except Exception as e:
            logger.error(f"Failed to load FAISS index: {e}")
            return None
    
    def save_documents(
        self,
        documents: List,
        filename: str = "documents.pkl"
    ) -> bool:
        """
        Save document list to DBFS or local storage.
        
        Args:
            documents: List of documents to save
            filename: Name of the pickle file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.is_databricks:
                file_path = os.path.join(self.dbfs_mount_point, filename)
            else:
                file_path = os.path.join(self.local_cache_dir, filename)
            
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'wb') as f:
                pickle.dump(documents, f)
            
            logger.info(f"✅ Documents saved: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save documents: {e}")
            return False
    
    def load_documents(self, filename: str = "documents.pkl") -> Optional[List]:
        """
        Load document list from DBFS or local storage.
        
        Args:
            filename: Name of the pickle file
            
        Returns:
            List of documents or None if not found
        """
        try:
            if self.is_databricks:
                file_path = os.path.join(self.dbfs_mount_point, filename)
            else:
                file_path = os.path.join(self.local_cache_dir, filename)
            
            if not os.path.exists(file_path):
                logger.warning(f"Documents file not found: {file_path}")
                return None
            
            with open(file_path, 'rb') as f:
                documents = pickle.load(f)
            
            logger.info(f"✅ Documents loaded from: {file_path}")
            return documents
            
        except Exception as e:
            logger.error(f"Failed to load documents: {e}")
            return None
    
    def upload_file_to_dbfs(
        self,
        local_path: str,
        dbfs_path: str
    ) -> bool:
        """
        Upload a file to DBFS.
        
        Args:
            local_path: Local file path
            dbfs_path: Target DBFS path (relative to mount point)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.is_databricks:
                # Use dbutils in Databricks
                full_dbfs_path = os.path.join(self.dbfs_mount_point, dbfs_path)
                
                # In Databricks notebook, you would use:
                # dbutils.fs.cp(f"file:{local_path}", full_dbfs_path)
                
                # For now, use regular file copy
                import shutil
                os.makedirs(os.path.dirname(full_dbfs_path), exist_ok=True)
                shutil.copy2(local_path, full_dbfs_path)
                
                logger.info(f"✅ File uploaded to DBFS: {full_dbfs_path}")
                return True
            else:
                # In local mode, just copy to cache
                import shutil
                dest_path = os.path.join(self.local_cache_dir, dbfs_path)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copy2(local_path, dest_path)
                
                logger.info(f"✅ File copied to cache: {dest_path}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            return False
    
    def download_file_from_dbfs(
        self,
        dbfs_path: str,
        local_path: str
    ) -> bool:
        """
        Download a file from DBFS.
        
        Args:
            dbfs_path: DBFS path (relative to mount point)
            local_path: Target local path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.is_databricks:
                full_dbfs_path = os.path.join(self.dbfs_mount_point, dbfs_path)
                
                # In Databricks notebook, you would use:
                # dbutils.fs.cp(full_dbfs_path, f"file:{local_path}")
                
                import shutil
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                shutil.copy2(full_dbfs_path, local_path)
                
                logger.info(f"✅ File downloaded from DBFS: {full_dbfs_path}")
                return True
            else:
                # In local mode, copy from cache
                import shutil
                source_path = os.path.join(self.local_cache_dir, dbfs_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                shutil.copy2(source_path, local_path)
                
                logger.info(f"✅ File copied from cache: {source_path}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            return False
    
    def list_files(self, path: str = "") -> List[str]:
        """
        List files in DBFS directory.
        
        Args:
            path: Directory path (relative to mount point)
            
        Returns:
            List of file paths
        """
        try:
            if self.is_databricks:
                full_path = os.path.join(self.dbfs_mount_point, path)
            else:
                full_path = os.path.join(self.local_cache_dir, path)
            
            if not os.path.exists(full_path):
                return []
            
            files = []
            for root, dirs, filenames in os.walk(full_path):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(file_path, full_path)
                    files.append(rel_path)
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def delete_file(self, path: str) -> bool:
        """
        Delete a file from DBFS or local storage.
        
        Args:
            path: File path (relative to mount point)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.is_databricks:
                full_path = os.path.join(self.dbfs_mount_point, path)
            else:
                full_path = os.path.join(self.local_cache_dir, path)
            
            if os.path.exists(full_path):
                os.remove(full_path)
                logger.info(f"✅ File deleted: {full_path}")
                return True
            else:
                logger.warning(f"File not found: {full_path}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            return False


class FAISSManager:
    """
    High-level manager for FAISS index with DBFS integration.
    Combines FAISS operations with cloud storage.
    """
    
    def __init__(
        self,
        dbfs_storage: DBFSStorage,
        embedding_model_name: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize FAISS manager.
        
        Args:
            dbfs_storage: DBFSStorage instance
            embedding_model_name: Name of the embedding model
        """
        self.dbfs_storage = dbfs_storage
        self.embedding_model_name = embedding_model_name
        self.index = None
        self.documents = []
    
    def initialize_index(self, dimension: int = 384):
        """
        Initialize a new FAISS index.
        
        Args:
            dimension: Embedding dimension (default for all-MiniLM-L6-v2)
        """
        import faiss
        self.index = faiss.IndexFlatL2(dimension)
        logger.info(f"Initialized new FAISS index with dimension {dimension}")
    
    def add_embeddings(self, embeddings, documents):
        """
        Add embeddings and documents to index.
        
        Args:
            embeddings: Numpy array of embeddings
            documents: List of corresponding documents
        """
        import numpy as np
        
        if self.index is None:
            self.initialize_index(dimension=embeddings.shape[1])
        
        self.index.add(embeddings.astype('float32'))
        self.documents.extend(documents)
        
        logger.info(f"Added {len(documents)} documents to index. Total: {len(self.documents)}")
    
    def save_to_dbfs(self, index_name: str = "faiss_index.index") -> bool:
        """Save index and documents to DBFS."""
        success = self.dbfs_storage.save_faiss_index(self.index, index_name)
        if success:
            success = self.dbfs_storage.save_documents(
                self.documents,
                filename=index_name.replace('.index', '_documents.pkl')
            )
        return success
    
    def load_from_dbfs(self, index_name: str = "faiss_index.index") -> bool:
        """Load index and documents from DBFS."""
        self.index = self.dbfs_storage.load_faiss_index(index_name)
        if self.index is None:
            return False
        
        documents = self.dbfs_storage.load_documents(
            filename=index_name.replace('.index', '_documents.pkl')
        )
        if documents is None:
            return False
        
        self.documents = documents
        logger.info(f"✅ Loaded FAISS index with {len(self.documents)} documents")
        return True
    
    def search(self, query_embedding, top_k: int = 5):
        """
        Search for similar documents.
        
        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return
            
        Returns:
            List of (document, score) tuples
        """
        import numpy as np
        
        if self.index is None or len(self.documents) == 0:
            logger.warning("Index is empty")
            return []
        
        query_embedding = np.array([query_embedding]).astype('float32')
        distances, indices = self.index.search(query_embedding, top_k)
        
        results = []
        for i, idx in enumerate(indices[0]):
            if idx < len(self.documents):
                results.append((self.documents[idx], float(distances[0][i])))
        
        return results
