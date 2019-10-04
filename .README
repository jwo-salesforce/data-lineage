## create the job->file dependency graph

### Run Sumo Query "com.krux.common.mr.base.KruxJob (main): M/R Job Path!" with your desired day range, and export the result as CSV, it should look like resources/log-sample.csv
### Update the MRInputOutputLogParserTest main(), and point it to the downloaded file, and run it.

### Load the output to Neo4j
```
 MATCH (n) DETACH DELETE n;
 load csv from "https://krux-knowledge.s3.amazonaws.com/public/filenodes.txt" as line  merge (:file {name:line[0]});
 load csv from "https://krux-knowledge.s3.amazonaws.com/public/jobnodes.txt" as line  merge (:job {name:line[0]});
 load csv from "https://krux-knowledge.s3.amazonaws.com/public/inputedges.txt" as line  match(input:file {name:line[0]}) match(job:job {name:line[3]}) merge (input)-[:input]->(job);
 load csv from "https://krux-knowledge.s3.amazonaws.com/public/outputedges.txt" as line  match(job:job {name:line[0]}) match(output:file {name:line[3]}) merge (job)-[:output]->(output);
 You can explore the graph by querying in the graph browser: where the name should be a key in either filenodes.txt and jobnodes.txt above.
 match (n {name:"s3://krux-org-{uuid}/organization-user-match/consolidated-uberid-kuid"}) return n
```