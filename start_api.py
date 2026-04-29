"""
Launch the Prague Real Estate Intelligence API + dashboard.
Run from the project root:
    python start_api.py
Then open http://localhost:8000 in your browser.
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
