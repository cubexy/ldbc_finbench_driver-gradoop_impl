## How to get data?

Data is stored in the `src/main/resources/gradoop/data` directory. The data is stored in CSV format.

The data format is as follows:

```
/snapshot - CSV snapshot data
/incremental - CSV incremental data
/read_params - CSV read input data
/gradoop-graph-data - Gradoop graph data
```

### Snapshot Data & Incremental Data

Incremental and snapshot data are generated using the Finbench DataGen.
Here, scale factor and other parameters can be provided.

A list of pre-generated datasets can also be found
here: [Finbench DataGen Datasets (Google Drive)](https://drive.google.com/drive/folders/1NIAo4KptskBytbXoOqmF3Sto4hTX3JIH)

_Note: If you do generate these parameters yourself, the method that writes the data to the CSV sometimes saves floats
as with a decimal comma rather than a point. (This probably has to do with the locale settings of the machine that
generated the data.) Using this generated data causes the Finbench importer to fail. This can probably be fixed by
matching all commas surrounded by numbers (___[0-9],[0-9]___) and replacing the decimal comma with a decimal point._

### Read parameters

Hidden deep inside the DataGen, the read parameters are generated using ./tools/paramgen/params_gen.py.
Here, the input directory (snapshot dir) and output directory (read_params dir) can be provided.

_Note: Sometimes the read params cause the program to go ham - for whatever reason (I have not had the time to refactor
the entire code just to find and fix this.) A re-run or two fixes this!_

### Gradoop Graph Data

The Gradoop graph data is generated using the Gradoop Finbench Importer.
Here, the input directory (snapshot dir) and output directory (gradoop-graph-data dir) can be provided.