package com.csforge.sstable.bootstrap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Resolved Java home, executable, and specification major version. */
public final class JavaInstallation {
    private static final Pattern VERSION_OUTPUT = Pattern.compile(
            "(?:java|openjdk) version \\\"([^\\\"]+)\\\"");

    private final Path home;
    private final Path executable;
    private final String version;
    private final int majorVersion;

    private JavaInstallation(Path home, Path executable, String version, int majorVersion) {
        this.home = home;
        this.executable = executable;
        this.version = version;
        this.majorVersion = majorVersion;
    }

    public static JavaInstallation discover(Path explicitHome,
                                            Map<String, String> environment,
                                            Properties systemProperties) throws BootstrapException {
        Path home;
        if (explicitHome != null) {
            home = explicitHome;
        } else if (hasText(environment.get("JAVA_HOME"))) {
            home = Paths.get(environment.get("JAVA_HOME"));
        } else if (hasText(systemProperties.getProperty("java.home"))) {
            home = Paths.get(systemProperties.getProperty("java.home"));
        } else {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot locate Java; use --java-home or JAVA_HOME");
        }

        Path realHome = realDirectory(home, "Java home");
        Path executable = realHome.resolve("bin").resolve(isWindows() ? "java.exe" : "java");
        if (!Files.isRegularFile(executable) || !Files.isExecutable(executable)) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Java executable is missing or not executable: " + executable);
        }

        String version = readReleaseVersion(realHome);
        if (version == null) {
            version = runVersionCommand(executable);
        }
        return new JavaInstallation(realHome, canonical(executable), version, parseMajor(version));
    }

    private static String readReleaseVersion(Path home) throws BootstrapException {
        Path releaseFile = home.resolve("release");
        if (!Files.isRegularFile(releaseFile)) {
            return null;
        }

        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(releaseFile)) {
            properties.load(input);
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot read Java identity file " + releaseFile, e);
        }
        String version = properties.getProperty("JAVA_VERSION");
        return version == null ? null : unquote(version.trim());
    }

    private static String runVersionCommand(Path executable) throws BootstrapException {
        Process process;
        try {
            process = new ProcessBuilder(executable.toString(), "-version")
                    .redirectErrorStream(true)
                    .start();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot run " + executable + " -version", e);
        }

        String output;
        try (InputStream input = process.getInputStream()) {
            output = readUtf8(input);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                        executable + " -version exited with code " + exitCode);
            }
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot read Java version from " + executable, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Interrupted while reading Java version from " + executable, e);
        }

        Matcher matcher = VERSION_OUTPUT.matcher(output);
        if (!matcher.find()) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot parse Java version output from " + executable);
        }
        return matcher.group(1);
    }

    private static String readUtf8(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int count;
        while ((count = input.read(buffer)) >= 0) {
            output.write(buffer, 0, count);
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    static int parseMajor(String version) throws BootstrapException {
        String[] parts = version.split("[._+-]");
        String major = parts.length > 1 && "1".equals(parts[0]) ? parts[1] : parts[0];
        try {
            int result = Integer.parseInt(major);
            if (result <= 0) {
                throw new NumberFormatException("not positive");
            }
            return result;
        } catch (NumberFormatException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot parse Java release version '" + version + "'", e);
        }
    }

    private static Path realDirectory(Path path, String description) throws BootstrapException {
        try {
            Path real = path.toRealPath();
            if (!Files.isDirectory(real)) {
                throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                        description + " is not a directory: " + path);
            }
            return real;
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    description + " does not exist or cannot be resolved: " + path, e);
        }
    }

    private static Path canonical(Path path) throws BootstrapException {
        try {
            return path.toRealPath();
        } catch (IOException e) {
            throw new BootstrapException(BootstrapException.DISCOVERY_EXIT_CODE,
                    "Cannot resolve path " + path, e);
        }
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    public Path home() {
        return home;
    }

    public Path executable() {
        return executable;
    }

    public String version() {
        return version;
    }

    public int majorVersion() {
        return majorVersion;
    }
}
