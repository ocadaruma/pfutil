package com.mayreh.pfutil;

import java.nio.file.Files;
import java.nio.file.Paths;

public class TestUtil {
    public static byte[] getResourceAsBytes(String name) throws Exception {
        return Files.readAllBytes(Paths.get(
                TestUtil.class.getClassLoader().getResource(name).toURI()));
    }
}
