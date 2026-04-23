from src.pipeline import run_pipeline

if __name__ == "__main__":
    choice = input("Run Bezrealitky as well? (y/n): ").strip().lower()
    include_bez = choice == "y"
    summary = run_pipeline(include_bezrealitky=include_bez)
    print("\nPipeline summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
