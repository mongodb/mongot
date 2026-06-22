package com.xgen.mongot.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;

public class SecretsParser {

  private static final List<PosixFilePermission> POSIX_OWNER_PERMISSIONS =
      List.of(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);

  /**
   * Reads a secret from a file. Verifies the file has owner-only permissions (on POSIX). Returns
   * the file content with leading and trailing whitespace trimmed, so common formats (e.g. a single
   * line saved with a trailing newline) work as intended.
   */
  public static String readSecretFile(Path filePath) throws IOException {
    Check.checkArg(filePath.toFile().exists(), "Secret file %s does not exist", filePath);
    Check.checkArg(filePath.toFile().isFile(), "Secret file %s is not a file", filePath);

    try {
      var filePermissions = Files.getPosixFilePermissions(filePath);

      POSIX_OWNER_PERMISSIONS.forEach(filePermissions::remove);
      Check.checkArg(
          filePermissions.isEmpty(),
          "Secret file %s permissions are too permissive (must only be readable by owner)",
          filePath);
    } catch (UnsupportedOperationException ignored) {
      // Not all filesystems support POSIX permissions, we won't verify file permissions on those
      // filesystems as the number of edge cases increase significantly.
    }

    return Files.readString(filePath).trim();
  }
}
