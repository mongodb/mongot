package com.xgen.mongot.index.lucene.vector;

import com.google.errorprone.annotations.Var;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Similarity {
  private static final Logger LOG = LoggerFactory.getLogger(Similarity.class);
  private static final @Nullable MethodHandle DOT_I8;
  private static final @Nullable MethodHandle L2_I8;
  private static final @Nullable MethodHandle COS_I8;

  /**
   * Set to true if the underlying native implementations are hardware accelerated.
   *
   * <p>This may be false on older hardware where low usage rate makes it hard to justify writing an
   * implementation. It is also false on platforms where the {@code mongot_vecsim} native library is
   * unavailable (e.g. macOS x86_64 builds that ship without the shared library). Callers must
   * always check this flag before invoking {@link #dotI8}, {@link #l2I8}, or {@link #cosI8};
   * calling those methods when this flag is {@code false} will throw {@link NullPointerException}.
   * {@link #prepareVector} is pure JVM and is safe to call regardless of this flag.
   */
  public static final boolean I8_ACCELERATION;

  /**
   * Load native dependencies of this library.
   */
  public static void load() {
    // noop, handled by static block.
  }

  static {
    @Var MethodHandle dotI8 = null;
    @Var MethodHandle l2I8 = null;
    @Var MethodHandle cosI8 = null;
    @Var boolean accel = false;
    try {
      loadNativeLibrary();

      Linker linker = Linker.nativeLinker();
      SymbolLookup lookup = SymbolLookup.loaderLookup();

      FunctionDescriptor i8Desc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.JAVA_LONG);
      FunctionDescriptor cosI8Desc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_FLOAT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.JAVA_LONG);

      dotI8 = linker.downcallHandle(lookup.find("mongot_vecsim_dot_i8").orElseThrow(), i8Desc);
      l2I8 = linker.downcallHandle(lookup.find("mongot_vecsim_l2_i8").orElseThrow(), i8Desc);
      cosI8 = linker.downcallHandle(lookup.find("mongot_vecsim_cos_i8").orElseThrow(), cosI8Desc);
      MethodHandle i8Accelerated =
          linker.downcallHandle(
              lookup.find("mongot_vecsim_i8_accelerated").orElseThrow(),
              FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN));
      accel = (boolean) i8Accelerated.invokeExact();
    } catch (UnsatisfiedLinkError e) {
      LOG.atWarn().setCause(e).log(
          "mongot_vecsim native library unavailable; falling back to non-native vector scoring");
    } catch (Throwable t) {
      throw new ExceptionInInitializerError(t);
    }
    DOT_I8 = dotI8;
    L2_I8 = l2I8;
    COS_I8 = cosI8;
    I8_ACCELERATION = accel;
  }

  static final String LIB_NAME = "mongot_vecsim";

  /**
   * Load the mongot_vecsim shared library. Resolution order:
   *
   * <ol>
   *   <li>Bazel runfiles via {@code RUNFILES_DIR} environment variable
   *   <li>Bazel test runfiles via {@code TEST_SRCDIR} environment variable
   *   <li>bazel-bin fallback for direct bazel run invocations
   *   <li>Sibling {@code lib/} directory relative to the JAR containing this class
   *   <li>Search {@code java.library.path}
   * </ol>
   */
  private static void loadNativeLibrary() {
    String libName = System.mapLibraryName(LIB_NAME);
    String bazelRelative = "src/main/c/" + libName;

    // Search the bazel runfiles tree for the library. If it is not present then attempt to load
    // from the library path -- this is the typical configuration when deployed.
    Optional<Path> found =
        Stream.of(
                resolveFromEnv("RUNFILES_DIR", bazelRelative),
                resolveFromEnv("TEST_SRCDIR", "_main/" + bazelRelative),
                Optional.of(Path.of("bazel-bin", bazelRelative)),
                resolveFromJarSibling(libName))
            .flatMap(Optional::stream)
            .filter(Files::isRegularFile)
            .findFirst();

    try {
      if (found.isPresent()) {
        System.load(found.get().toAbsolutePath().toString());
      } else {
        System.loadLibrary(LIB_NAME);
      }
    } catch (UnsatisfiedLinkError e) {
      throw new UnsatisfiedLinkError(
          "Cannot find native library "
              + libName
              + " in bazel runfiles or in java library paths. Original error: "
              + e);
    }
  }

  private static Optional<Path> resolveFromEnv(String envVar, String relative) {
    return Optional.ofNullable(System.getenv(envVar))
        .filter(dir -> !dir.isEmpty())
        .map(dir -> Path.of(dir).resolve(relative));
  }

  // Resolves <libName> relative to the lib/ directory sibling of the JAR containing this class,
  // e.g. /mongot-community/bin/mongot_community_deploy.jar -> /mongot-community/lib/<libName>.
  private static Optional<Path> resolveFromJarSibling(String libName) {
    try {
      URL location = Similarity.class.getProtectionDomain().getCodeSource().getLocation();
      if (location == null) {
        return Optional.empty();
      }
      Path jarDir = Path.of(location.toURI()).getParent();
      if (jarDir == null) {
        return Optional.empty();
      }
      Path jarParent = jarDir.getParent();
      if (jarParent == null) {
        return Optional.empty();
      }
      return Optional.of(jarParent.resolve("lib").resolve(libName));
    } catch (URISyntaxException | InvalidPathException e) {
      LOG.atWarn().setCause(e).log("Failed to resolve native library path from JAR sibling");
      return Optional.empty();
    }
  }

  public static MemorySegment prepareVector(Arena arena, byte[] vector) {
    MemorySegment seg = arena.allocate(vector.length, 1);
    MemorySegment.copy(vector, 0, seg, ValueLayout.JAVA_BYTE, 0, vector.length);
    return seg;
  }

  private static final String NATIVE_UNAVAILABLE_MESSAGE =
      "mongot_vecsim native library unavailable; check Similarity.I8_ACCELERATION before calling";

  public static int dotI8(MemorySegment a, MemorySegment b) {
    if (a.byteSize() != b.byteSize()) {
      throw new IllegalArgumentException("Vectors must have the same size");
    }
    MethodHandle handle = Objects.requireNonNull(DOT_I8, NATIVE_UNAVAILABLE_MESSAGE);
    try {
      return (int) handle.invokeExact(a, b, a.byteSize());
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  public static int l2I8(MemorySegment a, MemorySegment b) {
    if (a.byteSize() != b.byteSize()) {
      throw new IllegalArgumentException("Vectors must have the same size");
    }
    MethodHandle handle = Objects.requireNonNull(L2_I8, NATIVE_UNAVAILABLE_MESSAGE);
    try {
      return (int) handle.invokeExact(a, b, a.byteSize());
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  public static float cosI8(MemorySegment a, MemorySegment b) {
    if (a.byteSize() != b.byteSize()) {
      throw new IllegalArgumentException("Vectors must have the same size");
    }
    MethodHandle handle = Objects.requireNonNull(COS_I8, NATIVE_UNAVAILABLE_MESSAGE);
    try {
      return (float) handle.invokeExact(a, b, a.byteSize());
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }
}
