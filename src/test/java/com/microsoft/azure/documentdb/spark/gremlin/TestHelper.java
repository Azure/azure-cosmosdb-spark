package com.microsoft.azure.documentdb.spark.gremlin;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class TestHelper {

    private static final String SEP = File.separator;
    private static final char URL_SEP = '/';
    public static final String TEST_DATA_RELATIVE_DIR = "test-case-data";

    private TestHelper() {
    }

    /**
     * Creates a {@link File} reference that points to a directory relative to the supplied class in the
     * {@code /target} directory. Each {@code childPath} passed introduces a new sub-directory and all are placed
     * below the {@link #TEST_DATA_RELATIVE_DIR}.  For example, calling this method with "a", "b", and "c" as the
     * {@code childPath} arguments would yield a relative directory like: {@code test-case-data/clazz/a/b/c}. It is
     * a good idea to use the test class for the {@code clazz} argument so that it's easy to find the data if
     * necessary after test execution.
     */
    public static File makeTestDataPath(final Class clazz, final String... childPath) {
        final File root = getRootOfBuildDirectory(clazz);
        final List<String> cleanedPaths = Stream.of(childPath).map(TestHelper::cleanPathSegment).collect(Collectors.toList());

        // use the class name in the directory structure
        cleanedPaths.add(0, cleanPathSegment(clazz.getSimpleName()));

        final File f = new File(root, TEST_DATA_RELATIVE_DIR + SEP + String.join(SEP, cleanedPaths));
        if (!f.exists()) f.mkdirs();
        return f;
    }

    /**
     * Internally calls {@link #makeTestDataPath(Class, String...)} but returns the path as a string with the system
     * separator appended to the end.
     */
    public static String makeTestDataDirectory(final Class clazz, final String... childPath) {
        return makeTestDataPath(clazz, childPath).getAbsolutePath() + SEP;
    }

    /**
     * Gets and/or creates the root of the test data directory.  This  method is here as a convenience and should not
     * be used to store test data.  Use {@link #makeTestDataPath(Class, String...)} instead.
     */
    public static File getRootOfBuildDirectory(final Class clazz) {
        // build.dir gets sets during runs of tests with maven via the surefire configuration in the pom.xml
        // if that is not set as an environment variable, then the path is computed based on the location of the
        // requested class.  the computed version at least as far as intellij is concerned comes drops it into
        // /target/test-classes.  the build.dir had to be added because maven doesn't seem to like a computed path
        // as it likes to find that path in the .m2 directory and other weird places......
        final String buildDirectory = System.getProperty("build.dir");
        final File root = null == buildDirectory ? new File(computePath(clazz)).getParentFile() : new File(buildDirectory);
        if (!root.exists()) root.mkdirs();
        return root;
    }

    private static String computePath(final Class clazz) {
        final String clsUri = clazz.getName().replace('.', URL_SEP) + ".class";
        final URL url = clazz.getClassLoader().getResource(clsUri);
        String clsPath;
        try {
            clsPath = new File(url.toURI()).getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException("Unable to computePath for " + clazz, e);
        }
        return clsPath.substring(0, clsPath.length() - clsUri.length());
    }

    /**
     * Removes characters that aren't acceptable in a file path (mostly for windows).
     */
    public static String cleanPathSegment(final String toClean) {
        final String cleaned = toClean.replaceAll("[.\\\\/:*?\"<>|\\[\\]\\(\\)]", "");
        if (cleaned.length() == 0)
            throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
        return cleaned;
    }
}
