from pathlib import Path

from config import DOCS_DIR


def load_documents():
    documents = []
    for path in sorted(Path(DOCS_DIR).glob("*.md")):
        documents.append({"source": path.name, "text": path.read_text(encoding="utf-8")})
    return documents


def retrieve_context(question, top_k=3):
    documents = load_documents()
    if not documents:
        return []

    query_terms = {term.lower().strip(".,?!") for term in question.split() if len(term) > 3}
    scored = []
    for doc in documents:
        text = doc["text"].lower()
        score = sum(text.count(term) for term in query_terms)
        scored.append((score, doc))

    ranked = [doc for score, doc in sorted(scored, key=lambda item: item[0], reverse=True) if score > 0]
    return ranked[:top_k] or documents[:top_k]


def format_context(question, top_k=3):
    snippets = []
    for doc in retrieve_context(question, top_k=top_k):
        text = " ".join(doc["text"].split())
        snippets.append(f"Source: {doc['source']}\n{text[:1200]}")
    return "\n\n".join(snippets)

