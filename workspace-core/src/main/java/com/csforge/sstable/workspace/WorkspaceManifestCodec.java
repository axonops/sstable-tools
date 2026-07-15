package com.csforge.sstable.workspace;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Strict JSON codec with explicit validation for the versioned manifest schema. */
public final class WorkspaceManifestCodec {
    private static final Gson WRITER = new GsonBuilder()
            .disableHtmlEscaping()
            .serializeNulls()
            .setPrettyPrinting()
            .create();

    public byte[] encode(WorkspaceManifest manifest) {
        String json = WRITER.toJson(toJson(manifest)) + "\n";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public WorkspaceManifest decode(byte[] bytes) throws WorkspaceException {
        return decode(new String(bytes, StandardCharsets.UTF_8));
    }

    public WorkspaceManifest decode(String json) throws WorkspaceException {
        JsonElement root;
        try (Reader input = new StringReader(json);
             JsonReader reader = new JsonReader(input)) {
            reader.setStrictness(Strictness.STRICT);
            root = JsonParser.parseReader(reader);
        } catch (IOException | JsonParseException | IllegalStateException e) {
            throw new WorkspaceException("Malformed workspace manifest JSON", e);
        }
        if (!root.isJsonObject()) {
            throw new WorkspaceException("Workspace manifest root must be a JSON object");
        }
        return fromJson(root.getAsJsonObject());
    }

    private static JsonObject toJson(WorkspaceManifest manifest) {
        JsonObject root = new JsonObject();
        root.addProperty("formatVersion", manifest.formatVersion());
        root.addProperty("workspaceId", manifest.workspaceId().toString());
        root.addProperty("state", manifest.state().name());
        addNullable(root, "lastStableState", manifest.lastStableState() == null
                ? null : manifest.lastStableState().name());
        addNullable(root, "failureMessage", manifest.failureMessage());
        root.addProperty("createdAt", manifest.createdAt().toString());
        root.addProperty("updatedAt", manifest.updatedAt().toString());
        root.add("sourceInventory", sourceInventoryToJson(manifest.sourceInventory()));
        root.add("schemaIdentity", mapToJson(manifest.schemaIdentity()));
        root.add("runtimeIdentity", mapToJson(manifest.runtimeIdentity()));
        root.add("outputIdentity", mapToJson(manifest.outputIdentity()));
        root.add("baselineInventory", filesToJson(manifest.baselineInventory()));

        JsonArray exports = new JsonArray();
        for (ExportRecord record : manifest.exports()) {
            JsonObject item = new JsonObject();
            item.addProperty("exportId", record.exportId().toString());
            item.addProperty("createdAt", record.createdAt().toString());
            item.addProperty("outputFormat", record.outputFormat());
            item.add("files", filesToJson(record.files()));
            exports.add(item);
        }
        root.add("exports", exports);
        return root;
    }

    private static JsonObject sourceInventoryToJson(SourceInventory inventory) {
        JsonObject result = new JsonObject();
        JsonArray sets = new JsonArray();
        for (SstableSet set : inventory.sets()) {
            JsonObject item = new JsonObject();
            item.addProperty("descriptor", set.descriptor());
            item.addProperty("formatVersion", set.formatVersion());
            item.addProperty("format", set.format());
            item.addProperty("directory", set.directory().toString());
            JsonArray components = new JsonArray();
            for (SourceComponent component : set.components()) {
                JsonObject componentJson = new JsonObject();
                componentJson.addProperty("name", component.name());
                componentJson.addProperty("path", component.path().toString());
                componentJson.addProperty("size", component.size());
                componentJson.addProperty("sha256", component.sha256());
                components.add(componentJson);
            }
            item.add("components", components);
            sets.add(item);
        }
        result.add("sets", sets);
        return result;
    }

    private static JsonArray filesToJson(List<ManifestFile> files) {
        JsonArray result = new JsonArray();
        for (ManifestFile file : files) {
            JsonObject item = new JsonObject();
            item.addProperty("relativePath", file.relativePath());
            item.addProperty("size", file.size());
            item.addProperty("sha256", file.sha256());
            result.add(item);
        }
        return result;
    }

    private static JsonObject mapToJson(Map<String, String> map) {
        JsonObject result = new JsonObject();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            result.addProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static void addNullable(JsonObject object, String name, String value) {
        if (value == null) {
            object.add(name, JsonNull.INSTANCE);
        } else {
            object.addProperty(name, value);
        }
    }

    private static WorkspaceManifest fromJson(JsonObject root) throws WorkspaceException {
        requireOnly(root, "manifest", "formatVersion", "workspaceId", "state",
                "lastStableState", "failureMessage", "createdAt", "updatedAt",
                "sourceInventory", "schemaIdentity", "runtimeIdentity", "outputIdentity",
                "baselineInventory", "exports");
        int formatVersion = requiredInt(root, "formatVersion");
        UUID workspaceId = parseUuid(requiredString(root, "workspaceId"), "workspaceId");
        WorkspaceState state = parseState(requiredString(root, "state"), "state");
        WorkspaceState lastStableState = nullableString(root, "lastStableState") == null
                ? null : parseState(nullableString(root, "lastStableState"), "lastStableState");
        String failureMessage = nullableString(root, "failureMessage");
        Instant createdAt = parseInstant(requiredString(root, "createdAt"), "createdAt");
        Instant updatedAt = parseInstant(requiredString(root, "updatedAt"), "updatedAt");
        SourceInventory inventory = sourceInventoryFromJson(
                requiredObject(root, "sourceInventory"));
        Map<String, String> schema = stringMap(requiredObject(root, "schemaIdentity"),
                "schemaIdentity");
        Map<String, String> runtime = stringMap(requiredObject(root, "runtimeIdentity"),
                "runtimeIdentity");
        Map<String, String> output = stringMap(requiredObject(root, "outputIdentity"),
                "outputIdentity");
        List<ManifestFile> baseline = filesFromJson(requiredArray(root, "baselineInventory"),
                "baselineInventory");
        List<ExportRecord> exports = exportsFromJson(requiredArray(root, "exports"));
        return new WorkspaceManifest(formatVersion, workspaceId, state, lastStableState,
                failureMessage, createdAt, updatedAt, inventory, schema, runtime, output,
                baseline, exports);
    }

    private static SourceInventory sourceInventoryFromJson(JsonObject object)
            throws WorkspaceException {
        requireOnly(object, "sourceInventory", "sets");
        JsonArray values = requiredArray(object, "sets");
        if (values.size() == 0) {
            throw new WorkspaceException("sourceInventory.sets must not be empty");
        }
        List<SstableSet> sets = new ArrayList<>();
        Set<String> descriptorPaths = new HashSet<>();
        for (int index = 0; index < values.size(); index++) {
            JsonObject item = arrayObject(values, index, "sourceInventory.sets");
            requireOnly(item, "sourceInventory.sets[" + index + "]", "descriptor",
                    "formatVersion", "format", "directory", "components");
            String descriptor = requiredString(item, "descriptor");
            String version = requiredString(item, "formatVersion");
            String format = requiredString(item, "format");
            Path directory = absoluteNormalizedPath(requiredString(item, "directory"),
                    "source directory");
            String descriptorPath = directory + "/" + descriptor;
            if (!descriptorPaths.add(descriptorPath)) {
                throw new WorkspaceException("Duplicate source descriptor " + descriptorPath);
            }

            JsonArray componentValues = requiredArray(item, "components");
            if (componentValues.size() == 0) {
                throw new WorkspaceException("Source descriptor has no components: " + descriptor);
            }
            List<SourceComponent> components = new ArrayList<>();
            Set<String> componentNames = new HashSet<>();
            for (int componentIndex = 0; componentIndex < componentValues.size(); componentIndex++) {
                JsonObject component = arrayObject(componentValues, componentIndex, "components");
                requireOnly(component, "component", "name", "path", "size", "sha256");
                String name = requiredString(component, "name");
                if (!componentNames.add(name)) {
                    throw new WorkspaceException("Duplicate source component " + name);
                }
                Path path = absoluteNormalizedPath(requiredString(component, "path"),
                        "source component");
                if (!directory.equals(path.getParent())
                        || !path.getFileName().toString().equals(descriptor + "-" + name)) {
                    throw new WorkspaceException("Source component path does not match descriptor: "
                            + path);
                }
                long size = requiredLong(component, "size");
                String sha256 = requiredString(component, "sha256");
                validateFileIdentity(path.toString(), size, sha256);
                components.add(new SourceComponent(name, path, size, sha256));
            }
            sets.add(new SstableSet(descriptor, version, format, directory, components));
        }
        return new SourceInventory(sets);
    }

    private static List<ManifestFile> filesFromJson(JsonArray values, String description)
            throws WorkspaceException {
        List<ManifestFile> files = new ArrayList<>();
        Set<String> paths = new HashSet<>();
        for (int index = 0; index < values.size(); index++) {
            JsonObject item = arrayObject(values, index, description);
            requireOnly(item, description + "[" + index + "]", "relativePath", "size", "sha256");
            ManifestFile file = new ManifestFile(requiredString(item, "relativePath"),
                    requiredLong(item, "size"), requiredString(item, "sha256"));
            if (!paths.add(file.relativePath())) {
                throw new WorkspaceException("Duplicate manifest file " + file.relativePath());
            }
            files.add(file);
        }
        return files;
    }

    private static List<ExportRecord> exportsFromJson(JsonArray values)
            throws WorkspaceException {
        List<ExportRecord> exports = new ArrayList<>();
        Set<UUID> ids = new HashSet<>();
        for (int index = 0; index < values.size(); index++) {
            JsonObject item = arrayObject(values, index, "exports");
            requireOnly(item, "exports[" + index + "]", "exportId", "createdAt",
                    "outputFormat", "files");
            UUID id = parseUuid(requiredString(item, "exportId"), "exportId");
            if (!ids.add(id)) {
                throw new WorkspaceException("Duplicate export ID " + id);
            }
            exports.add(new ExportRecord(id,
                    parseInstant(requiredString(item, "createdAt"), "export.createdAt"),
                    requiredString(item, "outputFormat"),
                    filesFromJson(requiredArray(item, "files"), "export.files")));
        }
        return exports;
    }

    private static Map<String, String> stringMap(JsonObject object, String description)
            throws WorkspaceException {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            if (entry.getKey().trim().isEmpty() || !isString(entry.getValue())) {
                throw new WorkspaceException(description + " values must be strings");
            }
            result.put(entry.getKey(), entry.getValue().getAsString());
        }
        return result;
    }

    private static void requireOnly(JsonObject object, String description, String... fields)
            throws WorkspaceException {
        Set<String> allowed = new HashSet<>(Arrays.asList(fields));
        for (String key : object.keySet()) {
            if (!allowed.contains(key)) {
                throw new WorkspaceException("Unknown " + description + " field: " + key);
            }
        }
        for (String field : fields) {
            if (!object.has(field)) {
                throw new WorkspaceException("Missing " + description + " field: " + field);
            }
        }
    }

    private static JsonObject requiredObject(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonObject()) {
            throw new WorkspaceException(name + " must be a JSON object");
        }
        return value.getAsJsonObject();
    }

    private static JsonArray requiredArray(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonArray()) {
            throw new WorkspaceException(name + " must be a JSON array");
        }
        return value.getAsJsonArray();
    }

    private static JsonObject arrayObject(JsonArray array, int index, String description)
            throws WorkspaceException {
        JsonElement value = array.get(index);
        if (!value.isJsonObject()) {
            throw new WorkspaceException(description + "[" + index + "] must be an object");
        }
        return value.getAsJsonObject();
    }

    private static String requiredString(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (!isString(value) || value.getAsString().trim().isEmpty()) {
            throw new WorkspaceException(name + " must be a non-empty string");
        }
        return value.getAsString();
    }

    private static String nullableString(JsonObject object, String name)
            throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!isString(value)) {
            throw new WorkspaceException(name + " must be a string or null");
        }
        return value.getAsString();
    }

    private static boolean isString(JsonElement value) {
        return value != null && value.isJsonPrimitive()
                && value.getAsJsonPrimitive().isString();
    }

    private static int requiredInt(JsonObject object, String name) throws WorkspaceException {
        long value = requiredLong(object, name);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new WorkspaceException(name + " is outside the integer range");
        }
        return (int) value;
    }

    private static long requiredLong(JsonObject object, String name) throws WorkspaceException {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw new WorkspaceException(name + " must be a JSON number");
        }
        try {
            String number = value.getAsString();
            if (number.contains(".") || number.contains("e") || number.contains("E")) {
                throw new NumberFormatException("not an integer");
            }
            return Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw new WorkspaceException(name + " must be an integer", e);
        }
    }

    private static Path absoluteNormalizedPath(String value, String description)
            throws WorkspaceException {
        Path path;
        try {
            path = Paths.get(value);
        } catch (RuntimeException e) {
            throw new WorkspaceException("Invalid " + description + " path " + value, e);
        }
        if (!path.isAbsolute() || !path.equals(path.normalize())) {
            throw new WorkspaceException(description + " path must be absolute and normalized: "
                    + value);
        }
        return path;
    }

    private static void validateFileIdentity(String path, long size, String sha256)
            throws WorkspaceException {
        if (size < 0 || sha256 == null || !sha256.matches("[0-9a-f]{64}")) {
            throw new WorkspaceException("Invalid file identity for " + path);
        }
    }

    private static UUID parseUuid(String value, String description) throws WorkspaceException {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new WorkspaceException("Invalid " + description + " UUID " + value, e);
        }
    }

    private static Instant parseInstant(String value, String description)
            throws WorkspaceException {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new WorkspaceException("Invalid " + description + " timestamp " + value, e);
        }
    }

    private static WorkspaceState parseState(String value, String description)
            throws WorkspaceException {
        try {
            return WorkspaceState.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new WorkspaceException("Invalid " + description + " value " + value, e);
        }
    }
}
