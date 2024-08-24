## How to get data?

Data is stored in the `src/main/resources/gradoop/data` directory. The data is stored in CSV format. The data format is as follows:

```
/snapshot - CSV snapshot data
/incremental - CSV incremental data
/read_params - CSV read input data
/gradoop-graph-data - Gradoop graph data
```

### Snapshot Data & Incremental Data

Incremental and snapshot data are generated using the Finbench DataGen.
Here, scale factor and other parameters can be provided.

A list of pre-generated datasets can also be found here: [Finbench DataGen Datasets (Google Drive)](https://drive.google.com/drive/folders/1NIAo4KptskBytbXoOqmF3Sto4hTX3JIH)

### Read parameters

Hidden deep inside the DataGen, the read parameters are generated using ./tools/paramgen/params_gen.py.
Here, the input directory (snapshot dir) and output directory (read_params dir) can be provided.

### Gradoop Graph Data

The Gradoop graph data is generated using the Gradoop Finbench Importer.
Here, the input directory (snapshot dir) and output directory (gradoop-graph-data dir) can be provided.