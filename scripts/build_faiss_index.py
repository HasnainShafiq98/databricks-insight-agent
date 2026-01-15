"""
Build FAISS index from PDF documents in data/documents folder.
This creates a searchable knowledge base for the RAG system.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from src.intelligence.document_processor import DocumentLoader, DocumentChunker
from src.intelligence.context_retriever import ContextRetriever, Document
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Build FAISS index from PDF documents."""
    
    print("=" * 80)
    print("📚 FAISS INDEX BUILDER - Document Processing")
    print("=" * 80)
    
    load_dotenv()
    
    # Configuration
    documents_path = os.getenv('DOCUMENTS_PATH', './data/documents')
    faiss_index_path = os.getenv('FAISS_INDEX_PATH', './data/faiss_index.faiss')
    
    print(f"\n📁 Documents folder: {documents_path}")
    print(f"💾 FAISS index output: {faiss_index_path}")
    
    # Check if documents folder exists
    if not os.path.exists(documents_path):
        print(f"\n❌ Documents folder not found: {documents_path}")
        print("   Please create the folder and add your PDF documents.")
        return
    
    # List PDF files
    pdf_files = [f for f in os.listdir(documents_path) if f.endswith('.pdf')]
    
    if not pdf_files:
        print(f"\n⚠️  No PDF files found in {documents_path}")
        print("   Please add PDF documents to process.")
        return
    
    print(f"\n✅ Found {len(pdf_files)} PDF file(s):")
    for pdf in pdf_files:
        print(f"   • {pdf}")
    
    # Initialize components
    print("\n" + "=" * 80)
    print("STEP 1: Loading Documents")
    print("=" * 80)
    
    # Create chunker first, then pass it to loader
    chunker = DocumentChunker(chunk_size=500, chunk_overlap=50)
    loader = DocumentLoader(chunker)
    
    all_chunks = []
    
    # Try to import PyPDF2 for PDF handling
    try:
        from PyPDF2 import PdfReader
        pdf_available = True
    except ImportError:
        print("\n⚠️  PyPDF2 not installed. Installing now...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
        from PyPDF2 import PdfReader
        pdf_available = True
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(documents_path, pdf_file)
        print(f"\n📄 Processing: {pdf_file}")
        
        try:
            # Extract text from PDF using PyPDF2
            reader = PdfReader(pdf_path)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            
            if not text or len(text.strip()) < 10:
                print(f"   ⚠️  No text extracted (might be image-based PDF)")

                continue
            
            print(f"   ✅ Extracted {len(text)} characters")
            
            # Chunk the text
            chunks = chunker.chunk_text(
                text=text,
                source=pdf_file,
                metadata={
                    "source_file": pdf_file,
                    "document_type": "business_knowledge"
                }
            )
            
            print(f"   ✅ Created {len(chunks)} chunks")
            all_chunks.extend(chunks)
            
        except Exception as e:
            print(f"   ❌ Error processing {pdf_file}: {e}")
    
    if not all_chunks:
        print("\n❌ No chunks created. Cannot build FAISS index.")
        print("   Make sure your PDFs contain extractable text.")
        return
    
    print(f"\n" + "=" * 80)
    print(f"STEP 2: Building FAISS Index")
    print("=" * 80)
    print(f"\n📊 Total chunks to index: {len(all_chunks)}")
    
    # Initialize FAISS retriever
    context_retriever = ContextRetriever(
        embedding_model="all-MiniLM-L6-v2",
        index_path=faiss_index_path
    )
    
    # Convert chunks to documents
    documents = [
        Document(
            content=chunk.content,
            metadata=chunk.metadata
        )
        for chunk in all_chunks
    ]
    
    # Add to FAISS index
    print("\n🔨 Building FAISS index (this may take a moment)...")
    context_retriever.add_documents(documents)
    
    # Save index
    print(f"\n💾 Saving index to: {faiss_index_path}")
    context_retriever.save_index(faiss_index_path)
    
    print("\n" + "=" * 80)
    print("✅ FAISS INDEX BUILT SUCCESSFULLY!")
    print("=" * 80)
    print(f"\n📊 Statistics:")
    print(f"   • Documents processed: {len(pdf_files)}")
    print(f"   • Total chunks: {len(all_chunks)}")
    print(f"   • Index saved to: {faiss_index_path}")
    
    # Test the index
    print("\n" + "=" * 80)
    print("STEP 3: Testing Index")
    print("=" * 80)
    
    test_queries = [
        "What are the key performance indicators?",
        "Tell me about customer segments",
        "What are the business rules?"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Test query: '{query}'")
        results = context_retriever.search(query, top_k=2)
        
        if results:
            print(f"   ✅ Found {len(results)} relevant chunks:")
            for i, (doc, score) in enumerate(results, 1):
                preview = doc.content[:100].replace('\n', ' ')
                print(f"      {i}. [Score: {score:.3f}] {preview}...")
        else:
            print("   ⚠️  No results found")
    
    print("\n" + "=" * 80)
    print("🎉 ALL DONE!")
    print("=" * 80)
    print("\nYour FAISS index is ready to use with the Databricks Insight Agent.")
    print("The agent will now be able to retrieve relevant business context from your PDFs!")


if __name__ == "__main__":
    main()
