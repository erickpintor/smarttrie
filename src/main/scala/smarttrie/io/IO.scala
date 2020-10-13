package smarttrie.io

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

object IO {

  def listFiles(dir: Path, predicate: Path => Boolean): List[Path] = {
    val res = List.newBuilder[Path]
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path] {
        override def visitFile(
            file: Path,
            attrs: BasicFileAttributes
        ): FileVisitResult = {
          if (predicate(file)) res += file
          FileVisitResult.CONTINUE
        }
      }
    )
    res.result()
  }

  def listFiles(dir: Path, extension: String): List[Path] =
    listFiles(dir, _.toString.endsWith(extension))

  def cleanDirectory(dir: Path, removeDir: Boolean = true): Unit =
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path] {
        override def visitFile(
            file: Path,
            attrs: BasicFileAttributes
        ): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(
            dir: Path,
            exc: IOException
        ): FileVisitResult = {
          if (removeDir) Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
}
