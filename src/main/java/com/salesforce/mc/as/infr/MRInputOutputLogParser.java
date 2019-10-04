package com.salesforce.mc.as.infr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Parsing sumologic logs which gives input and output path for a particular job.
 * Sumo Query
 * "com.krux.common.mr.base.KruxJob (main): M/R Job Path!"
 */

public class MRInputOutputLogParser {

    public static void main(String[] args) throws Exception {

        String path = args[1];
        Set<FileNode> fileNodes = new HashSet<>();
        Set<JobNode> jobNodes = new HashSet<>();
        Set<Edge> inputEdges = new HashSet<>();
        Set<Edge> outputEdges = new HashSet<>();

        Files.lines(Paths.get(new URI(path.toString()))).filter(line -> line.split(" ").length > 2)
                .map(line -> {
                    String[] fields = line.split(" ");
                    return Arrays.toString(Arrays.copyOfRange(fields, 8, fields.length));
                })
                .forEach(line -> {
                    String[] values = line.split(" ");
                    String output = (values.length == 3 ? values[2] : "-").replaceAll("\\[outputPath=", "").replaceAll("\\]\"\\]", "");
                    final String job = values[0].replaceAll("\\[\\[jobName=", "").replaceAll("(],)", "");
                    FileNode outputFile = new FileNode(output);
                    JobNode jobNode = new JobNode(job);

                    fileNodes.add(outputFile);
                    jobNodes.add(jobNode);
                    outputEdges.add(new Edge(jobNode, "output", outputFile));
                    Arrays.stream(values[1].split(","))
                            .map(r -> r.replaceAll("\\[inputPath=", "").replaceAll("(])", ""))
                            .forEach(input -> {
                                FileNode inputFile = new FileNode(input);
                                fileNodes.add(inputFile);
                                inputEdges.add(new Edge(inputFile, "input", jobNode));

                            });
                });

        //need manually add output edges as the output log is truncated. This is usually not a problem for most of the jobs.
        outputEdges.add(new Edge( new JobNode("com.krux.cdim.mr.BridgeKeyUserMatchConsolidator"), "output", new FileNode("s3://krux-org-{uuid}/cdim/consolidated-unfiltered-org-bridge-key-usermatch/{date}")));
        outputEdges.add(new Edge( new JobNode("com.krux.cdim.mr.BridgeKeyUserMatchOrganizationFilterAndConsolidate"), "output", new FileNode("s3://krux-org-{uuid}/cdim/consolidated-org-bridge-key-usermatch/{date}")));
        outputEdges.add(new Edge( new JobNode("com.krux.cdim.mr.BridgeKeyUserMatchOrganizationFilterAndConsolidate"), "output", new FileNode("s3://krux-org-{uuid}/cdim/consolidated-org-bridge-key-usermatch-v2/{date}")));

        final String basePath = "/Users/jwo/Library/Application Support/Neo4j Desktop/Application/neo4jDatabases/database-9aedf967-282c-4fac-93f0-a46eb5414e7b/installation-3.5.6/import";
        final BufferedWriter fileNodeWriter = new BufferedWriter(new FileWriter(basePath + "/filenodes.txt"));
        final BufferedWriter jobNodeWriter = new BufferedWriter(new FileWriter(basePath + "/jobnodes.txt"));
        final BufferedWriter inputEdgeWriter = new BufferedWriter(new FileWriter(basePath + "/inputedges.txt"));
        final BufferedWriter outputEdgeWriter = new BufferedWriter(new FileWriter(basePath + "/outputedges.txt"));
        fileNodes.forEach(n-> write(n, fileNodeWriter));
        jobNodes.forEach(n -> write(n, jobNodeWriter));
        inputEdges.forEach(n -> write(n, inputEdgeWriter));
        outputEdges.forEach(n -> write(n, outputEdgeWriter));
        fileNodeWriter.close();
        jobNodeWriter.close();
        inputEdgeWriter.close();
        outputEdgeWriter.close();
    }

    private static void write(Object n, BufferedWriter writer) {
        try {
            writer.write(n.toString());
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Node {
        String id;
        String label;

        public Node(String id, String label) {
            this.id = id;
            this.label = label;
        }

        public String toString() {
            return id + "," + label;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(id, node.id) &&
                    Objects.equals(label, node.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, label);
        }
    }

    public static class FileNode extends Node {

        public FileNode(String id) {
            super(canonicalize(id), "file");
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        static String canonicalize(String filePath) {
            filePath = filePath.endsWith("/") ? filePath.substring(0, filePath.length() - 1) : filePath;
            filePath = filePath.replaceAll("[0-9a-fA-Z]{8}-[0-9a-fA-Z]{4}-[0-9a-fA-Z]{4}-[0-9a-fA-Z]{4}-[0-9a-fA-Z]{12}", "{uuid}").replaceAll("201[4-9]-[0-9]{2}-[0-9]{2}", "{date}").replaceAll("/[0-9a-zA-Z_-]{8}$", "").replaceAll("/[0-9a-zA-Z_-]{8}/", "/{id}/");

            //canonicalize specific filepaths
            if(filePath.startsWith("s3://krux-cdim/daily-org-partitioned-bridge-key-usermatch/{date}/")) {
                filePath = filePath.replaceAll("[0-9a-fA-Z]{8}-[0-9a-fA-Z]{4}-[0-9a-fA-Z]{4}-", "{uuid}");
            }
            if(filePath.startsWith("s3://krux-application-temp/partner-segment-populations/{date}/{uuid}/")) {
                filePath= "s3://krux-application-temp/partner-segment-populations/{date}/{uuid}";
            }
            if(filePath.startsWith("s3://krux-application-temp/user-attributes/first-party-data/{uuid}/") && filePath.endsWith("/current")) {
                filePath = "s3://krux-application-temp/user-attributes/first-party-data/{uuid}/${id}/current";
            }
            if(filePath.startsWith("s3://krux-audience-segments/{uuid}/custom-segments/custom-output-")&& filePath.endsWith("/new")) {
                filePath = "s3://krux-audience-segments/{uuid}/custom-segments/custom-output/new";
            }
            if(filePath.startsWith("s3://krux-consent-logs/{date}/")) {
                filePath = filePath.replaceAll("/[0-9]{2}/", "/{num}/");
            }
            if(filePath.startsWith("s3://krux-ml/lookalikes/naive-bayes/{uuid}/classified/segment-processing/")) {
                filePath = filePath.replaceAll("/[0-9]{14}$","" );
            }
            if(filePath.startsWith("s3://krux-offline-imports")) {
                filePath = filePath.replaceAll("/[a-zA-Z0-9_-]{9}/", "/{id}/");
            }
            if(filePath.startsWith("s3://krux-partner-management/organization-user-link-segment-map/{uuid}")) {
                filePath = filePath.replaceAll("/[0-9a-zA-Z]{24}", "/{id}");
            }
            if(filePath.startsWith("s3://krux-partner-management/partner-segment-populations/incremental/{date}/{uuid}/")) {
                filePath = "s3://krux-partner-management/partner-segment-populations/incremental/{date}/{uuid}";
            }
            if(filePath.startsWith("s3://krux-data-providers/{uuid}/") && filePath.endsWith("/current")) {
                filePath = filePath.replaceAll("uuid}/[0-9A-Za-z_]*/current$", "uuid}/{id}/current").replaceAll("/[0-9]{6,7}/","/{id}/");
            }
            if(filePath.startsWith("s3://krux-partner-management/partner-user-segment-map/full/{date}/{uuid}/")) {
                filePath = filePath.replaceAll("uuid}/[a-z_]*/consolidated/", "uuid}/{id}/consolidated/").replaceAll("uuid}/[a-z_]*$","uuid}");
            }
            if(filePath.startsWith("s3://krux-partner-management/partner-user-segment-map/update/{date}/{uuid}/")) {
                filePath = filePath.replaceAll("uuid}/[a-z_]*/", "uuid}/{id}/");
            }
            if(filePath.startsWith("s3://krux-partner-management/usermatch-tables/dynamo/{date}/")) {
                filePath = filePath.replaceAll("/[0-9]{1,2}","/{num}");
            }
            if(filePath.startsWith("s3://krux-partners/client-")) {
                filePath= filePath.replaceAll("s3://krux-partners/client-[0-9a-z\\-]*/","s3://krux-partners/client-{partner}/");
            }
            if(filePath.startsWith("s3://krux-tables/partner-user-match/daily-current/{date}/current-90/")) {
                filePath= "s3://krux-tables/partner-user-match/daily-current/{date}/current-90";
            }
            if(filePath.startsWith("s3://krux-tables/user-attributes/first-party-data/{uuid}/")) {
                filePath = filePath.replaceAll("uuid}/[a-z0-9\\-]*/", "uuid}/{id}/");
            }
            if(filePath.startsWith("s3://krux-partner-management/usermatch-tables/dynamo/{date}/{num}/cumulative/")) {
                filePath = filePath.replaceAll("cumulative/[0-9a-zA-Z_]*$", "cumulative");
            }
            if(filePath.startsWith("s3://krux-partner-management/usermatch-tables/dynamo/{date}/{num}/delta/{date}/")) {
                filePath = filePath.replaceAll("date}/[0-9a-zA-Z_]*$", "date}");
            }
            if(filePath.startsWith("s3://krux-pending-logs/")) {
                filePath = filePath.replaceAll("/[0-9]{2}$", "/{hour}");
            }
            if(filePath.startsWith("s3://krux-stats/user-attributes/first-party-data/{id}/{uuid}/")&& filePath.endsWith("/current")) {
                filePath = "s3://krux-stats/user-attributes/first-party-data/{id}/{uuid}/{id}/current";
            }
            if(filePath.startsWith("s3://krux-stats/user-attributes/first-party-data/{uuid}/") && filePath.endsWith("/current")) {
                filePath = "s3://krux-stats/user-attributes/first-party-data/{uuid}/{id}/current";
            }
            if(filePath.startsWith("s3://krux-partners/client-{partner}/krux-internal/pandora-liveramp")) {
                filePath = "s3://krux-partners/client-{partner}/krux-internal/pandora-liveramp";
            }
            if(filePath.startsWith("s3://krux-partners/partner-")) {
                filePath = filePath.substring(0, filePath.indexOf("/", "s3://krux-partners/partner-".length()));
            }
            if(filePath.startsWith("s3://krux-consent/{id}/aggregated/")) {
                filePath = "s3://krux-consent/{id}/aggregated";
            }

            if(filePath.endsWith("\"")|| filePath.endsWith("\"]")) filePath  = "invalid";

            //there is a chance that uuid and id ended files are generated by multiple output. So let's treat them the same node.
            if(filePath.endsWith("/{uuid}")) {
                filePath = filePath.substring(0, filePath.length()-7);
            } else if (filePath.endsWith("/{id}")) {
                filePath = filePath.substring(0, filePath.length()-5);
            }
            return filePath;
        }
    }

    public static class JobNode extends Node {
        public JobNode(String id) {
            super(id, "job");
        }
    }


    public static class Edge {
        Node from;
        String rel;
        Node to;

        public Edge(Node from, String rel, Node to) {
            this.from = from;
            this.rel = rel;
            this.to = to;
        }

        @Override
        public String toString() {
            return from + "," + rel + "," + to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Edge edge = (Edge) o;
            return Objects.equals(from, edge.from) &&
                    Objects.equals(rel, edge.rel) &&
                    Objects.equals(to, edge.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, rel, to);
        }
    }

}


