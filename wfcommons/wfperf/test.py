from wfcommons.wfchef.recipes import BlastRecipe
from wfcommons.wfperf.perf import WorkflowBenchmark
import pathlib

def main():
    bench = WorkflowBenchmark(BlastRecipe, 300)
    bench.generate_input_file(pathlib.Path("test.txt"))
    bench.create_benchmark_from_input_file("test.txt")

if __name__ == "__main__":
    main()