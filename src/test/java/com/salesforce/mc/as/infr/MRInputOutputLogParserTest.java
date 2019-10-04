package com.salesforce.mc.as.infr;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class MRInputOutputLogParserTest {

    @Test
    public void main() throws Exception {
        //test run
        String path = MRInputOutputLogParser.class.getClassLoader()
                .getResource("log-sample.csv").toString();

        //Use this for your real data parsing.
        // MRInputOutputLogParser.main(new String[]{"", "file:/Users/jwo/Downloads/search-results-2019-06-07T10_21_17.957-0700.csv"});
    }




    @Test
    public void regexReplace() {
        System.out.println(new MRInputOutputLogParser.FileNode("s3://krux-partners/partner-pandora/albertsons_llc_601789/1345939/{date}/PID/incr"));
    }
}