package com.example.hbase.admin;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TableTest.class,
        FilterTest.class,
        ReadOnlyTest.class,
        DeleteTest.class

})
public class MainTest {
}
