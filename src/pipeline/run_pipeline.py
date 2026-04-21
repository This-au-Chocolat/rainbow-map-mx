from pipeline import run_full_pipeline


if __name__ == "__main__":
    outputs = run_full_pipeline()
    print("Pipeline complete. Generated tables:")
    for name, frame in outputs.items():
        print(f"- {name}: {frame.shape}")
