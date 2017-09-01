package com.example.hbase.admin;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        ClusterTest.class,
        NameSpaceTest.class,
        DDLTest.class,
        DMLTest.class,
        FilterTest.class,
        CoprocessorTest.class,
        ReadOnlyTest.class,

})
public class MainTest {
}
