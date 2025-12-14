import dask.dataframe as dd

def run_distributed_processing(csv_path):
    print("\n========== DISTRIBUTED PROCESSING (DASK) ==========")

    ddf = dd.read_csv(csv_path)
    print(f"Partitions: {ddf.npartitions}")

    result = ddf.groupby("department")["salary"].mean().compute()
    print("Distributed Avg Salary by Department:")
    print(result)

    return result
